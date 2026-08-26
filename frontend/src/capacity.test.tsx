import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import { cleanup, render, screen } from '@testing-library/react'
import { afterEach, describe, expect, it } from 'vitest'
import { CapacityDisclosure, IdlePolicyFloor } from './App'
import type { CapacityDisclosureSnapshot, DemoSession } from './api/types'

afterEach(cleanup)

const MATCHED: CapacityDisclosureSnapshot = {
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

function sessionWith(capacity: CapacityDisclosureSnapshot | null | undefined): DemoSession {
  return { capacity } as unknown as DemoSession
}

describe('CapacityDisclosure', () => {
  it('stays behind a click instead of sitting on the proof surface', () => {
    const { container } = render(<CapacityDisclosure session={sessionWith(MATCHED)} />)
    const details = container.querySelector('details.capacity-disclosure')
    expect(details).not.toBeNull()
    expect((details as HTMLDetailsElement).open).toBe(false)
  })

  it('names both lanes with their configured capacity', () => {
    render(<CapacityDisclosure session={sessionWith(MATCHED)} />)
    expect(screen.getByText('Lakebase')).toBeTruthy()
    expect(screen.getByText('0.5–2 CU')).toBeTruthy()
    expect(screen.getByText('Aurora Serverless v2')).toBeTruthy()
    expect(screen.getByText('0–2 ACU')).toBeTruthy()
  })

  it('shows memory, engine version and idle policy for each lane', () => {
    render(<CapacityDisclosure session={sessionWith(MATCHED)} />)
    expect(screen.getByText('Memory · ~1–4 GB')).toBeTruthy()
    expect(screen.getByText('Engine · PostgreSQL 17 (major only)')).toBeTruthy()
    expect(screen.getByText('Engine · PostgreSQL 17.10')).toBeTruthy()
    expect(screen.getByText(/Auto-pause after 300s \(AWS documented minimum\)/)).toBeTruthy()
  })

  it('says plainly when the ceilings do not match', () => {
    render(
      <CapacityDisclosure
        session={sessionWith({
          ...MATCHED,
          matched: false,
          lanes: [
            MATCHED.lanes[0],
            { ...MATCHED.lanes[1], product: 'RDS PostgreSQL', configured: 'db.t4g.micro', memory: '1 GiB' },
          ],
        })}
      />,
    )
    expect(screen.getByText(/Ceilings do not match/)).toBeTruthy()
    expect(screen.getByText('db.t4g.micro')).toBeTruthy()
  })

  it('distinguishes a live control-plane reading from a configured value', () => {
    render(<CapacityDisclosure session={sessionWith(MATCHED)} />)
    expect(screen.getByText('Configured value · not read back this run')).toBeTruthy()
    expect(screen.getByText('Read from the live control plane')).toBeTruthy()
  })

  it('marks an unreported figure honestly rather than substituting a constant', () => {
    render(
      <CapacityDisclosure
        session={sessionWith({
          ...MATCHED,
          lanes: [{ ...MATCHED.lanes[0], basis: 'unreported' }],
        })}
      />,
    )
    expect(screen.getByText('Not reported by the control plane')).toBeTruthy()
  })

  it('says max connections are not published when the class is unknown', () => {
    render(
      <CapacityDisclosure
        session={sessionWith({
          ...MATCHED,
          lanes: [{ ...MATCHED.lanes[1], max_connections: null }],
        })}
      />,
    )
    expect(screen.getByText('Max connections · not published')).toBeTruthy()
  })

  it('colours the Lakebase corner red and the opponent corner blue', () => {
    const { container } = render(<CapacityDisclosure session={sessionWith(MATCHED)} />)
    expect(container.querySelector('article[data-corner="red"]')).not.toBeNull()
    expect(container.querySelector('article[data-corner="blue"]')).not.toBeNull()
  })

  it('renders a single lane for rounds with no AWS database', () => {
    const { container } = render(
      <CapacityDisclosure
        session={sessionWith({
          matched: true,
          summary: 'Lakebase 0.5–2 CU (~1–4 GB)',
          note: 'No Aurora or RDS database is provisioned for this round, so no compute comparison is made.',
          lanes: [MATCHED.lanes[0]],
        })}
      />,
    )
    expect(container.querySelectorAll('.capacity-lanes > article')).toHaveLength(1)
    expect(screen.getByText(/no compute comparison is made/)).toBeTruthy()
  })

  it('renders nothing when the server sent no capacity', () => {
    const { container } = render(<CapacityDisclosure session={sessionWith(null)} />)
    expect(container.innerHTML).toBe('')
  })
})

describe('IdlePolicyFloor', () => {
  it('frames the Aurora gap as a product floor, not a speed difference', () => {
    render(<IdlePolicyFloor competitorCannotIdle={false} />)
    expect(screen.getByText('Idle policy · not idle speed')).toBeTruthy()
    expect(screen.getByText(/240s of any gap is a product floor/)).toBeTruthy()
  })

  it('names both vendor minimums', () => {
    render(<IdlePolicyFloor competitorCannotIdle={false} />)
    const copy = screen.getByText(/shortest supported timeout/)
    expect(copy.textContent).toContain('Lakebase 60s')
    expect(copy.textContent).toContain('Aurora 300s')
    expect(copy.textContent).toContain('AWS will not accept below 300s')
  })

  it('does not claim Lakebase settles faster', () => {
    render(<IdlePolicyFloor competitorCannotIdle={false} />)
    const copy = screen.getByText(/shortest supported timeout/).textContent ?? ''
    expect(copy).not.toMatch(/faster|quicker|beats|wins/i)
  })

  it('says the RDS lane is not timed rather than giving it a number', () => {
    render(<IdlePolicyFloor competitorCannotIdle />)
    const copy = screen.getByText(/Provisioned RDS/)
    expect(copy.textContent).toContain('no automatic idle pause at all')
    expect(copy.textContent).toContain('not timed here')
    expect(copy.textContent).not.toContain('Aurora')
  })
})

/**
 * The summary line is a claim, and `matched` alone was not enough to earn it.
 *
 * Two ways it went out false. At session-create nothing has been read back, so
 * every lane's basis is `configured` and `matched` is computed from the
 * constants -- the whole pre-arm window asserted a parity nobody had observed.
 * And Rounds 4 and 6 have no AWS database, so their single lane returns
 * `matched` true meaning "nothing to mismatch", which rendered as a matched
 * ceiling on a round with no opposing box.
 *
 * Four verdicts, and the asymmetry between them is the point: only the
 * affirmative claim is gated on observation. A mismatch is reported from the
 * constants alone, because a mismatch in the constants is a real defect and
 * waiting for an arm that may never come would hide it.
 */
describe('CapacityDisclosure · what the summary is willing to claim', () => {
  const observed = (snapshot: CapacityDisclosureSnapshot): CapacityDisclosureSnapshot => ({
    ...snapshot,
    lanes: snapshot.lanes.map((lane) => ({ ...lane, basis: 'observed' as const })),
  })

  function summaryText(container: HTMLElement): string {
    const summary = container.querySelector('details.capacity-disclosure > summary')
    expect(summary).toBeTruthy()
    return summary?.textContent ?? ''
  }

  it('claims a matched ceiling only when every lane was read back', () => {
    const { container } = render(<CapacityDisclosure session={sessionWith(observed(MATCHED))} />)
    expect(summaryText(container)).toBe('Configured compute · Matched memory ceiling')
  })

  it('will not claim parity from the constants before anything is read back', () => {
    // MATCHED as the server builds it at session-create: `matched` is true, but
    // the Lakebase lane is still the configured value. This is the pre-arm
    // window, and it used to read "Matched memory ceiling" throughout.
    expect(MATCHED.matched).toBe(true)
    expect(MATCHED.lanes.some((lane) => lane.basis !== 'observed')).toBe(true)
    const { container } = render(<CapacityDisclosure session={sessionWith(MATCHED)} />)
    expect(summaryText(container)).toBe('Configured compute · Configured to match · not read back this run')
    expect(summaryText(container)).not.toMatch(/Matched memory ceiling/)
  })

  it('does not let an unreported lane buy the affirmative claim either', () => {
    const { container } = render(
      <CapacityDisclosure
        session={sessionWith({
          ...observed(MATCHED),
          lanes: [{ ...MATCHED.lanes[0], basis: 'unreported' }, observed(MATCHED).lanes[1]],
        })}
      />,
    )
    expect(summaryText(container)).toContain('not read back this run')
  })

  it('says there is no opposing box rather than calling one lane a match', () => {
    // Rounds 4 and 6. `matched` is true here and means nothing, so the lane
    // count is checked before it.
    const single: CapacityDisclosureSnapshot = {
      matched: true,
      summary: 'Lakebase 0.5–2 CU (~1–4 GB)',
      note: 'No Aurora or RDS database is provisioned for this round, so no compute comparison is made.',
      lanes: [{ ...MATCHED.lanes[0], basis: 'observed' }],
    }
    const { container } = render(<CapacityDisclosure session={sessionWith(single)} />)
    expect(summaryText(container)).toBe('Configured compute · No opposing box this round')
    expect(summaryText(container)).not.toMatch(/Matched|do not match/)
  })

  it('reports a mismatch from the constants without waiting to observe it', () => {
    // The asymmetry, asserted directly: basis is `configured` on both lanes and
    // the mismatch is still stated. Gating this branch on observation too would
    // hide a defect in the constants for the entire pre-arm window.
    const mismatched: CapacityDisclosureSnapshot = {
      ...MATCHED,
      matched: false,
      lanes: [
        { ...MATCHED.lanes[0], basis: 'configured' },
        { ...MATCHED.lanes[1], product: 'RDS PostgreSQL', configured: 'db.t4g.micro', memory: '1 GiB', basis: 'configured' },
      ],
    }
    const { container } = render(<CapacityDisclosure session={sessionWith(mismatched)} />)
    expect(summaryText(container)).toBe('Configured compute · Ceilings do not match')
  })

  it('still reports a mismatch once the lanes have been read back', () => {
    const { container } = render(
      <CapacityDisclosure session={sessionWith(observed({ ...MATCHED, matched: false }))} />,
    )
    expect(summaryText(container)).toBe('Configured compute · Ceilings do not match')
  })

  it('needs no new server field to decide any of this', () => {
    // `basis` is already on the wire per lane, so the verdict is computed from
    // the payload that exists rather than from one added for the summary line.
    const source = readFileSync(resolve(process.cwd(), 'src/App.tsx'), 'utf8')
    const component = source.slice(source.indexOf('export function CapacityDisclosure'))
    const body = component.slice(0, component.indexOf('\n}\n'))
    expect(body).toContain("lane.basis === 'observed'")
    expect(body).toContain('capacity.lanes.length > 1')
    // The lane count is consulted before `matched`, because Rounds 4 and 6 are
    // a `matched` true that is not about parity.
    expect(body.indexOf('hasOpposingLane')).toBeLessThan(body.indexOf('!capacity.matched'))
  })
})

/**
 * A disclosure nobody can reach discloses nothing. Neither component is
 * rendered by an existing test harness, so guard the call sites directly.
 */
describe('wiring', () => {
  const source = readFileSync(resolve(process.cwd(), 'src/App.tsx'), 'utf8')

  it('hangs the capacity disclosure off the Explain to the room overlay', () => {
    const overlay = source.slice(source.indexOf('function RingsideTake'))
    const body = overlay.slice(0, overlay.indexOf('\nfunction '))
    expect(body).toContain('<CapacityDisclosure session={session} />')
  })

  it('shows the idle-policy floor on the return-to-idle screen', () => {
    expect(source).toContain('<IdlePolicyFloor competitorCannotIdle={competitorCannotIdle}')
  })

  it('keeps the idle-policy floor off the cleanup variants of that screen', () => {
    expect(source).toContain('{!deleting && <IdlePolicyFloor')
  })
})
