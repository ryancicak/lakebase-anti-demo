/**
 * A cost scope's total says what it recurs on, from the lines that make it up.
 *
 * Every Round 5 receipt totals its installation overhead from one priced line, the runners'
 * gp3 volumes at 40 GB-month × $0.08. The total read "$3.20 one-time" directly above that
 * line's own "$3.20 / month".
 */

import { cleanup, render, screen } from '@testing-library/react'
import { afterEach, describe, expect, it } from 'vitest'

import type { CostLineItem, DemoSession } from './api/types'
import { CostReceiptDisclosure } from './App'

afterEach(() => {
  cleanup()
})

function overheadLine(overrides: Partial<CostLineItem>): CostLineItem {
  return {
    lane_id: 'shared',
    component: 'Two isolated runner gp3 root volumes',
    quantity: 40,
    unit: 'GB-month',
    unit_rate_usd: 0.08,
    reference_list_unit_rate_usd: null,
    subtotal_usd: 3.2,
    rate_basis: 'standard_list',
    cadence: 'month',
    status: 'estimate',
    scope: 'installation_overhead',
    source: 'AWS Price List API · AmazonEC2 · OnDemand · us-west-2',
    source_as_of: '2026-08-18T00:11:58Z',
    ...overrides,
  }
}

function sessionWith(lines: CostLineItem[], overhead: number | null): DemoSession {
  return {
    corners: ['cost'],
    cost_receipt: {
      currency: 'USD',
      region: 'us-west-2',
      status: 'estimate_only',
      reconciliation_status: 'estimate_only',
      known_bout_estimate_usd: null,
      known_monthly_carrying_cost_usd: null,
      known_installation_overhead_usd: overhead,
      lines,
    },
  } as unknown as DemoSession
}

describe('the installation overhead total', () => {
  it('recurs monthly when its priced lines do, as Round 5 runners do', () => {
    const lines = [
      overheadLine({
        component: 'Two isolated c7i.2xlarge runners',
        quantity: null,
        unit: 'instance-hour',
        unit_rate_usd: 0.357,
        subtotal_usd: null,
        status: 'usage_pending',
      }),
      overheadLine({}),
    ]
    render(<CostReceiptDisclosure session={sessionWith(lines, 3.2)} />)

    const totals = screen.getByLabelText('Cost scopes')
    expect(totals).toHaveTextContent(/Installation overhead\s*\$3\.20 \/ month/)
    expect(totals).not.toHaveTextContent(/one-time/)
  })

  it('names no cadence its priced lines do not share', () => {
    const lines = [overheadLine({}), overheadLine({ component: 'Setup', cadence: 'usage', subtotal_usd: 1 })]
    render(<CostReceiptDisclosure session={sessionWith(lines, 4.2)} />)

    const totals = screen.getByLabelText('Cost scopes')
    expect(totals).toHaveTextContent(/Installation overhead\s*\$4\.20(?! \/)/)
    expect(totals).not.toHaveTextContent(/one-time|Installation overhead\s*\$4\.20 \//)
  })
})
