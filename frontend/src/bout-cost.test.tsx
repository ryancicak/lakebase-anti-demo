// What a bout costs on Aurora, per round, and the two things this panel must
// never do to a number.
//
// WHY THIS FILE EXISTS. The cost model was corrected to stop inventing an
// Aurora quantity from a lane clock, which was right -- CloudWatch showed the
// clock missing 97.2% of Round 1's cost, inside the 300s auto-pause descent.
// The consequence was that every Aurora compute line in the product went
// `unavailable`. This panel is the render path that supplies the recorded
// measurements explicitly, so the two failure modes it has to be held to are
// the two that would undo the fix: a measured round quietly reverting to
// `unavailable`, and a missing quantity printing as `$0.00`.
//
// Rounds 4 and 6 are the one exception and it is a real one. They provision no
// Aurora cluster, so their zero is exact rather than absent, and the derivation
// saying so sits on the same element as the figure. That is the only condition
// under which a zero may be printed here.

import { readFileSync } from 'node:fs'
import { join, resolve } from 'node:path'
import { cleanup, render, screen } from '@testing-library/react'
import { afterEach, describe, expect, it } from 'vitest'
import { BoutCostDisclosure } from './App'
import type { BoutCostSnapshot, DemoSession } from './api/types'

afterEach(cleanup)

/** Mirrors what server/bout_cost.py builds. Figures from tests/test_bout_cost.py. */
const BOUT: BoutCostSnapshot = {
  total_display: '$0.130375 – $0.141727',
  superseded_display: '$0.049800 – $0.050340',
  dearest_claim:
    'On the Aurora lane, Round 5 is the dearest single round at $0.055549 \u2014 and still less than Rounds 2 and 3 combined, $0.070550. That comparison is the stronger claim and it is the one to make: the dearest round on this lane is still cheaper than two ordinary ones together.',
  lakebase_lane_claim:
    'On the Lakebase lane, Round 5 is the cheapest round \u2014 82 CU-seconds on its one isolable bout. Dearest against Aurora and cheapest on Lakebase are both measured and both true; they are different lanes, and neither figure is the other\u2019s answer.',
  summary:
    'Aurora\u2019s billed capacity, measured out of band rather than modelled from a lane clock. Every clock this harness keeps stops before Aurora does.',
  scope_note:
    'Aurora Serverless v2 compute, plus Round 5\u2019s RDS Proxy because that proxy exists only for the bout. The total takes Rounds 2 and 3 at the drain-billed reading; if AWS does not bill a deleting instance it falls to $0.079159 – $0.090511.',
  note:
    'The quantity is measured; the rate is not. ce:GetCostAndUsage and pricing:GetProducts are both denied to this principal, so every dollar here is rate-card derived, not invoice-verified.',
  rate_source:
    'CloudWatch ServerlessDatabaseCapacity \u00b7 us-west-2 \u00b7 AWS Price List API \u00b7 rate-card derived, not invoice-verified',
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
      derivation: '468.85 ACU-s \u00f7 3600 \u00d7 $0.12/ACU-hour',
      band_reason:
        'One bout, 7ECE1CB0. A 15.31s bout switched on 420s of billed capacity and 97.2% of it landed after the bell.',
      bouts: ['7ECE1CB0'],
    },
    {
      round_id: 'make_schema_change_safely',
      round_number: 2,
      label: 'Schema change, safely',
      provenance: 'measured',
      band_kind: 'unresolved_billing_question',
      usd_display: '$0.009067 – $0.043267',
      usd_low: 0.009067,
      usd_high: 0.043267,
      derivation: '272–1298 ACU-s \u00f7 3600 \u00d7 $0.12/ACU-hour',
      band_reason:
        'The instance reported a flat 2.0 ACU for minutes after DeleteDBInstance. Whether AWS bills a deleting instance is undocumented and ce:GetCostAndUsage is denied, so the band is a question, not a spread.',
      bouts: ['063A5187'],
    },
    {
      round_id: 'recover_deleted_order',
      round_number: 3,
      label: 'Recover a deleted order',
      provenance: 'measured',
      band_kind: 'unresolved_billing_question',
      usd_display: '$0.010267 – $0.027283',
      usd_low: 0.010267,
      usd_high: 0.027283,
      derivation: '308–818.49 ACU-s \u00f7 3600 \u00d7 $0.12/ACU-hour',
      band_reason:
        'Same unresolved deletion drain as Round 2. Both ends observed, neither settled.',
      bouts: ['A672140E'],
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
      band_reason:
        'Exact, not unavailable and not rounded down. There is no Aurora cluster to wake, so there is no capacity to bill.',
      bouts: [],
    },
    {
      round_id: 'survive_connection_spike',
      round_number: 5,
      label: 'Survive the connection spike',
      provenance: 'measured',
      band_kind: 'observed_spread',
      usd_display: '$0.044197 – $0.055549',
      usd_low: 0.044197,
      usd_high: 0.055549,
      derivation:
        '714.91–1017.48 ACU-s \u00f7 3600 \u00d7 $0.12/ACU-hour, plus RDS Proxy 611–649 s \u00d7 8 units \u00d7 $0.015/unit-hour',
      band_reason:
        'The band is the spread between two real bouts of this round, 42% apart, not modelling slack.',
      bouts: ['0123456789abcdef', 'abcdef0123456789'],
    },
    {
      round_id: 'analyze_live_orders_without_slowing_checkout',
      round_number: 6,
      label: 'Live app data into the lakehouse',
      provenance: 'structural_zero',
      band_kind: 'exact_zero',
      usd_display: '$0.00',
      usd_low: 0,
      usd_high: 0,
      derivation: 'infra/aws/locals.tf stands up no Aurora cluster for this round',
      band_reason:
        'Exact, not unavailable and not rounded down. There is no Aurora cluster to wake, so there is no capacity to bill.',
      bouts: [],
    },
  ],
}

function sessionWith(bout: BoutCostSnapshot | null): DemoSession {
  return { bout_cost: bout } as unknown as DemoSession
}

describe('BoutCostDisclosure', () => {
  it('puts the arithmetic behind a click, not on the arena screen', () => {
    const { container } = render(<BoutCostDisclosure session={sessionWith(BOUT)} />)
    const details = container.querySelector('details.bout-cost-disclosure')
    expect(details).toBeTruthy()
    expect(details?.hasAttribute('open')).toBe(false)
    expect(screen.getByText(/What a bout costs on Aurora/)).toBeTruthy()
  })

  it('renders a figure for every round, never the word unavailable', () => {
    // The regression this panel exists to close: with no caller supplying
    // samples, every one of these was `unavailable`.
    const { container } = render(<BoutCostDisclosure session={sessionWith(BOUT)} />)
    const rows = [...container.querySelectorAll('.bout-cost-rounds > article')]
    expect(rows).toHaveLength(6)
    for (const row of rows) {
      const figure = row.querySelector('b')?.textContent ?? ''
      expect(figure).toMatch(/^\$/)
      expect(figure.toLowerCase()).not.toContain('unavailable')
    }
  })

  it('shows each derivation so the figure can be checked', () => {
    render(<BoutCostDisclosure session={sessionWith(BOUT)} />)
    expect(screen.getByText('468.85 ACU-s ÷ 3600 × $0.12/ACU-hour')).toBeTruthy()
    expect(screen.getByText('272–1298 ACU-s ÷ 3600 × $0.12/ACU-hour')).toBeTruthy()
  })

  it('labels the measured rounds as measured and names the instrument', () => {
    const { container } = render(<BoutCostDisclosure session={sessionWith(BOUT)} />)
    const measured = [...container.querySelectorAll('article[data-provenance="measured"]')]
    expect(measured).toHaveLength(4)
    for (const row of measured) {
      expect(row.querySelector('span')?.textContent).toContain('Measured · CloudWatch')
    }
  })

  it('cites the bout ids a measured figure came from', () => {
    const { container } = render(<BoutCostDisclosure session={sessionWith(BOUT)} />)
    const cited = [...container.querySelectorAll('.bout-cost-rounds small')]
      .map((node) => node.textContent ?? '')
    expect(cited).toContain('Bouts · 7ECE1CB0')
    expect(cited).toContain('Bouts · 0123456789abcdef · abcdef0123456789')
    // The two structural zeros cite nothing, because there was no bout to name.
    expect(cited).toHaveLength(4)
  })

  it('renders nothing when the server sent no bout cost', () => {
    const { container } = render(<BoutCostDisclosure session={sessionWith(null)} />)
    expect(container.innerHTML).toBe('')
  })
})

describe('the exact zeros, which are not unavailables', () => {
  it('prints $0.00 for the two rounds that provision no Aurora', () => {
    const { container } = render(<BoutCostDisclosure session={sessionWith(BOUT)} />)
    const zeros = [...container.querySelectorAll('article[data-provenance="structural_zero"]')]
    expect(zeros).toHaveLength(2)
    for (const row of zeros) {
      expect(row.querySelector('b')?.textContent).toBe('$0.00')
    }
  })

  it('puts the reason for the zero on the same element as the zero', () => {
    // A bare $0.00 is indistinguishable from a failed lookup, which is the one
    // thing a cost panel may not be ambiguous about.
    const { container } = render(<BoutCostDisclosure session={sessionWith(BOUT)} />)
    for (const row of container.querySelectorAll('article[data-provenance="structural_zero"]')) {
      expect(row.querySelector('code')?.textContent).toContain('no Aurora cluster')
      expect(row.querySelector('em')?.textContent).toContain('not unavailable')
      expect(row.querySelector('span')?.textContent).toContain('Exact zero')
    }
  })

  it('marks the zero rows differently from the measured ones', () => {
    const { container } = render(<BoutCostDisclosure session={sessionWith(BOUT)} />)
    const kinds = [...container.querySelectorAll('.bout-cost-rounds > article')]
      .map((row) => row.getAttribute('data-provenance'))
    expect(kinds).toEqual([
      'measured',
      'measured',
      'measured',
      'structural_zero',
      'measured',
      'structural_zero',
    ])
  })

  it('never renders a zero for a round whose quantity is merely missing', () => {
    const broken: BoutCostSnapshot = {
      ...BOUT,
      rounds: [
        {
          ...BOUT.rounds[0],
          provenance: 'unavailable',
          band_kind: 'single_bout',
          usd_display: 'Unavailable',
          usd_low: null,
          usd_high: null,
        },
      ],
    }
    const { container } = render(<BoutCostDisclosure session={sessionWith(broken)} />)
    const row = container.querySelector('.bout-cost-rounds > article')
    expect(row?.querySelector('b')?.textContent).toBe('Unavailable')
    expect(row?.querySelector('b')?.textContent).not.toBe('$0.00')
    expect(row?.querySelector('span')?.textContent).toContain('not zero')
  })
})

describe('bands that must not collapse', () => {
  it('keeps Round 5 a spread across two real bouts', () => {
    const { container } = render(<BoutCostDisclosure session={sessionWith(BOUT)} />)
    const row = container.querySelector('article[data-band="observed_spread"]')
    expect(row?.querySelector('b')?.textContent).toBe('$0.044197 – $0.055549')
    expect(row?.querySelector('em')?.textContent).toContain('spread between two real bouts')
  })

  it('keeps Rounds 2 and 3 an unresolved question rather than a spread', () => {
    const { container } = render(<BoutCostDisclosure session={sessionWith(BOUT)} />)
    const rows = [...container.querySelectorAll('article[data-band="unresolved_billing_question"]')]
    expect(rows).toHaveLength(2)
    for (const row of rows) {
      expect(row.querySelector('b')?.textContent).toContain('–')
    }
    expect(rows[0].querySelector('em')?.textContent).toContain('undocumented')
  })

  it('tells the spread and the question apart in the markup', () => {
    // Identical-looking ranges, different epistemics. One rendering for both
    // would let a measured range read as an open question, or worse the reverse.
    const { container } = render(<BoutCostDisclosure session={sessionWith(BOUT)} />)
    const spread = container.querySelector('article[data-band="observed_spread"]')
    const question = container.querySelector('article[data-band="unresolved_billing_question"]')
    expect(spread?.getAttribute('data-band')).not.toBe(question?.getAttribute('data-band'))
  })

  it('never prints a single number for a banded round', () => {
    const { container } = render(<BoutCostDisclosure session={sessionWith(BOUT)} />)
    for (const row of container.querySelectorAll('.bout-cost-rounds > article')) {
      const band = row.getAttribute('data-band')
      if (band !== 'observed_spread' && band !== 'unresolved_billing_question') continue
      expect(row.querySelector('b')?.textContent).toContain('–')
    }
  })
})

describe('the figures it publishes', () => {
  it('leads with the measured six-round total', () => {
    render(<BoutCostDisclosure session={sessionWith(BOUT)} />)
    expect(screen.getByText(/\$0.130375 – \$0.141727 for six rounds/)).toBeTruthy()
  })

  it('retires the superseded figure in public rather than swapping it out', () => {
    // A 2.7x correction is one an audience may already have written down.
    render(<BoutCostDisclosure session={sessionWith(BOUT)} />)
    const superseded = screen.getByText(/Supersedes \$0.049800 – \$0.050340/)
    expect(superseded.textContent).toContain('97.2% of the')
    expect(superseded.textContent).toContain('only the quantity')
  })

  it('does not present the old figure as current anywhere', () => {
    const { container } = render(<BoutCostDisclosure session={sessionWith(BOUT)} />)
    const summary = container.querySelector('summary')?.textContent ?? ''
    expect(summary).not.toContain('$0.049800')
    expect(summary).toContain('$0.130375 – $0.141727')
  })

  it('states the cheaper reading of the unresolved drain rather than hiding it', () => {
    render(<BoutCostDisclosure session={sessionWith(BOUT)} />)
    const scope = screen.getByText(/does not bill a deleting instance/)
    expect(scope.textContent).toContain('$0.079159 – $0.090511')
  })

  it('separates a measured quantity from an unverified rate', () => {
    render(<BoutCostDisclosure session={sessionWith(BOUT)} />)
    const note = screen.getByText(/The quantity is measured; the rate is not/)
    expect(note.textContent).toContain('not invoice-verified')
  })

  it('never describes an AWS figure as invoice-verified', () => {
    const { container } = render(<BoutCostDisclosure session={sessionWith(BOUT)} />)
    const text = container.textContent ?? ''
    const mentions = text.match(/invoice[- ]verified/gi) ?? []
    const negated = text.match(/not invoice[- ]verified/gi) ?? []
    expect(mentions.length).toBeGreaterThan(0)
    expect(mentions).toHaveLength(negated.length)
  })
})

describe('superlatives name their lane', () => {
  it('says dearest against Aurora and cheapest on Lakebase, on the same panel', () => {
    // Both are measured and both are true. Said in two panels without naming
    // the lane, they contradict each other in front of the audience.
    render(<BoutCostDisclosure session={sessionWith(BOUT)} />)
    expect(screen.getByText(/On the Aurora lane, Round 5 is the dearest/)).toBeTruthy()
    expect(screen.getByText(/On the Lakebase lane, Round 5 is the cheapest/)).toBeTruthy()
  })

  it('reconciles the two rather than leaving the reader to collide them', () => {
    render(<BoutCostDisclosure session={sessionWith(BOUT)} />)
    const lane = screen.getByText(/On the Lakebase lane/)
    expect(lane.textContent).toContain('different lanes')
    expect(lane.textContent).toContain('both true')
  })

  it('makes the stronger comparison, not just the superlative', () => {
    // Derived from the rows rather than restated against the fixture. Asserting
    // the literal $0.070550 here would pass even if both the claim and the rows
    // it summarises drifted together, which is exactly the failure a 2.7x
    // correction with no failing test looked like.
    const pair = BOUT.rounds
      .filter((round) => round.round_number === 2 || round.round_number === 3)
      .reduce((total, round) => total + (round.usd_high ?? 0), 0)
    const dearest = BOUT.rounds.find((round) => round.round_number === 5)?.usd_high ?? 0
    expect(dearest).toBeLessThan(pair)

    render(<BoutCostDisclosure session={sessionWith(BOUT)} />)
    const claim = screen.getByText(/On the Aurora lane, Round 5 is the dearest/).textContent ?? ''
    expect(claim).toContain(`Rounds 2 and 3 combined, $${pair.toFixed(6)}`)
    expect(claim).toContain(`dearest single round at $${dearest.toFixed(6)}`)
    expect(claim).toContain('still less than')
  })

  it('leaves no superlative on this panel without a lane beside it', () => {
    const { container } = render(<BoutCostDisclosure session={sessionWith(BOUT)} />)
    for (const node of container.querySelectorAll('p, em')) {
      const text = node.textContent ?? ''
      if (!/dearest|cheapest|most expensive/i.test(text)) continue
      expect(text).toMatch(/aurora|lakebase/i)
    }
  })
})

describe('house rules', () => {
  const app = readFileSync(resolve(process.cwd(), 'src/App.tsx'), 'utf8')
  const css = readFileSync(join(import.meta.dirname, 'styles.css'), 'utf8')

  it('keeps the panel in What It Cost and out of the presenter cue', () => {
    const room = app.slice(app.indexOf('export function CostRoom'))
    const body = room.slice(0, room.indexOf('\nfunction '))
    expect(body).toContain('<BoutCostDisclosure session={session} />')
    const cue = app.slice(app.indexOf('function RingsideTake'))
    expect(cue.slice(0, cue.indexOf('/** One lane'))).not.toContain('<BoutCostDisclosure')
  })

  it('uses no purple and no VERIFIED slogan', () => {
    const { container } = render(<BoutCostDisclosure session={sessionWith(BOUT)} />)
    expect(container.textContent).not.toContain('VERIFIED')
    const block = css.slice(css.indexOf('.bout-cost-disclosure'))
    const scoped = block.slice(0, block.indexOf('@media'))
    expect(scoped).not.toMatch(/purple|violet|indigo|magenta/i)
  })

  it('keeps the panel square and bordered like its siblings', () => {
    const block = css.slice(css.indexOf('.bout-cost-disclosure {'))
    const scoped = block.slice(0, block.indexOf('}'))
    expect(scoped).toContain('border: 3px solid #46527c')
    expect(scoped).not.toMatch(/border-radius/)
  })

  it('collapses the six rows to one column on a narrow viewport', () => {
    // Six two-column rows of six-decimal dollars and 16-character bout ids do
    // not fit 390px side by side, and a horizontal scrollbar on a cost panel is
    // a broken panel.
    const narrow = css.slice(css.lastIndexOf('@media (max-width: 700px)'))
    expect(narrow).toContain('.bout-cost-rounds { grid-template-columns: 1fr; }')
  })

  it('gives every long unbreakable token somewhere to break', () => {
    const block = css.slice(css.indexOf('.bout-cost-disclosure'))
    const scoped = block.slice(0, block.indexOf('@media'))
    for (const selector of ['.bout-cost-rounds code', '.bout-cost-rounds small']) {
      const rule = scoped.slice(scoped.indexOf(selector))
      expect(rule.slice(0, rule.indexOf('}'))).toContain('overflow-wrap: anywhere')
    }
  })
})
