/**
 * The round summary.
 *
 * Two things are being protected here. The first is that this screen survives a
 * restart: it is built on sealed receipts rather than the in-memory session store,
 * so a round run this morning is still on the board this afternoon. The second is
 * that merging a live bout with the record never invents or promotes a result --
 * the latest run wins, never the best one.
 */

import { readFileSync } from 'node:fs'
import { join } from 'node:path'

import { cleanup, render, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, describe, expect, it, vi } from 'vitest'

import {
  type BoutReceipt,
  type RoundResult,
  roundsWithResult,
  summariseRounds,
  summaryDuration,
  verdictFor,
  winnerLabel,
} from './recap'
import { Summary } from './summary'

/** Built from the receipts actually on disk under .anti-demo-v7/receipts/. */
function receipt(overrides: Partial<BoutReceipt> = {}): BoutReceipt {
  return {
    receipt: '7ECE1CB0',
    session_id: '7ece1cb0-0000-0000-0000-000000000000',
    round_id: 'wake_idle_app',
    round_title: 'WAKE THIS IDLE APP',
    opponent: 'Aurora Serverless v2',
    opponent_id: 'aurora_serverless_v2',
    outcome: 'declared',
    sealing_event: 'run_finished',
    has_measurements: true,
    metric: 'bout_elapsed_ms',
    lakebase: { ms: 2399.8, state: 'verified', lower_bound: false, reason: null },
    opponent_lane: { ms: 14569.57, state: 'verified', lower_bound: false, reason: null },
    margin_ms: 12169.77,
    start_skew_ms: 0.25,
    sealed_at: '2026-08-21T00:05:02.631Z',
    remembered_result: null,
    failure: null,
    ...overrides,
  }
}

/** Receipt 434E2B99: Round 4 declared with no opponent lane at all. */
function roundFourReceipt(): BoutReceipt {
  return receipt({
    receipt: '434E2B99',
    session_id: '434e2b99-0000-0000-0000-000000000000',
    round_id: 'put_model_score_in_app',
    round_title: 'PUT A MODEL SCORE IN THE APP',
    lakebase: { ms: 12868.36, state: 'verified', lower_bound: false, reason: null },
    opponent_lane: { ms: null, state: 'not_supported', lower_bound: false, reason: null },
    margin_ms: null,
    start_skew_ms: null,
    sealed_at: '2026-08-21T00:59:40.635Z',
  })
}

function stubReceipts(receipts: BoutReceipt[]) {
  const fetchMock = vi.fn().mockImplementation((input: string) => {
    if (input === '/api/receipts') {
      return Promise.resolve({
        ok: true,
        status: 200,
        statusText: 'OK',
        json: () => Promise.resolve({ receipts }),
      })
    }
    throw new Error(`Unexpected request: ${input}`)
  })
  vi.stubGlobal('fetch', fetchMock)
  return fetchMock
}

afterEach(() => {
  // Auto-cleanup is off in this project's vitest config, so a leaked render
  // would make the next test's queries ambiguous rather than failing outright.
  cleanup()
  vi.unstubAllGlobals()
})

describe('merging the record with what is running now', () => {
  it('shows the latest run of a round, never the best one', () => {
    // The exact trap, and the reason this must not reuse the share card's
    // `headlineBout`: the earlier bout is the only one carrying a margin, so a
    // "best available result" rule would resurface it and quietly bury the run
    // the operator just did. The later bout is the honest answer.
    const flattering = receipt({
      receipt: 'AAAA0001',
      lakebase: { ms: 1000, state: 'verified', lower_bound: false, reason: null },
      opponent_lane: { ms: 90_000, state: 'verified', lower_bound: false, reason: null },
      margin_ms: 89_000,
      sealed_at: '2026-08-21T00:05:02.631Z',
    })
    const latest = receipt({
      receipt: 'AAAA0002',
      outcome: 'stopped_short',
      lakebase: { ms: 8000, state: 'verified', lower_bound: false, reason: null },
      opponent_lane: { ms: 12_000, state: 'failed', lower_bound: true, reason: null },
      margin_ms: null,
      sealed_at: '2026-08-21T03:00:00.000Z',
    })

    const [roundOne] = summariseRounds([flattering, latest])

    expect(roundOne.lakebaseMs).toBe(8000)
    expect(roundOne.status).toBe('lakebase_finished')
    // The buried bout's margin must not leak through onto the newer run.
    expect(roundOne.marginMs).toBeNull()
    expect(roundOne.boutsOnRecord).toBe(2)
  })

  it('lets a round that is running now win over its own earlier receipt', () => {
    const [first] = summariseRounds([receipt()], { roundId: 'wake_idle_app', running: true })

    expect(first.status).toBe('running')
    expect(first.source).toBe('live')
    // Nothing is silently dropped: the superseded bout is still counted.
    expect(first.boutsOnRecord).toBe(1)
  })

  it('leaves every other round on its receipt while one runs', () => {
    const results = summariseRounds(
      [receipt(), roundFourReceipt()],
      { roundId: 'wake_idle_app', running: true },
    )

    expect(results[0].status).toBe('running')
    expect(results[3].status).toBe('uncontested')
    expect(results[3].source).toBe('receipt')
  })

  it('keeps all six rounds in order, marking the ones never run', () => {
    const results = summariseRounds([receipt()])

    expect(results).toHaveLength(6)
    expect(results.map((result) => result.roundNumber))
      .toEqual(['01', '02', '03', '04', '05', '06'])
    expect(results[5].status).toBe('unrun')
    expect(results[5].source).toBeNull()
  })

  it('keeps an abandoned arm that measured nothing off the board without erasing it', () => {
    // Four of the twelve receipts on disk look exactly like this.
    const abandoned = receipt({
      receipt: 'C11F7591',
      outcome: 'stopped_short',
      has_measurements: false,
      lakebase: { ms: null, state: 'incomplete', lower_bound: false, reason: null },
      opponent_lane: { ms: null, state: 'incomplete', lower_bound: false, reason: null },
      margin_ms: null,
      sealed_at: '2026-08-21T00:04:20.922Z',
    })

    const round = summariseRounds([abandoned])[0]

    /* Not a result: no figure, no winner, and it never counts towards the rounds
       this installation can state a result for. That is the invariant.

       But not `unrun` either. Rendering an attempt somebody stopped identically
       to a round nobody selected is what made a toweled round vanish from the
       finale ledger, and an absent row on a scorecard reads as data loss rather
       than as a decision. */
    expect(round.status).toBe('abandoned')
    expect(round.lakebaseMs).toBeNull()
    expect(round.marginMs).toBeNull()
    expect(roundsWithResult(summariseRounds([abandoned]))).toBe(0)
  })

  it('separates a round nobody selected from one that was stopped', () => {
    const results = summariseRounds([receipt({
      round_id: 'make_schema_change_safely',
      outcome: 'stopped_short',
      has_measurements: false,
      lakebase: { ms: null, state: 'incomplete', lower_bound: false, reason: null },
      opponent_lane: { ms: null, state: 'incomplete', lower_bound: false, reason: null },
      margin_ms: null,
    })])

    expect(results[0].status).toBe('unrun')
    expect(results[1].status).toBe('abandoned')
  })

  it('reads an unfinished opponent lane as a floor, not a finish time', () => {
    const bounded = receipt({
      round_id: 'recover_deleted_order',
      round_title: 'RECOVER A DELETED ORDER',
      outcome: 'stopped_short',
      lakebase: { ms: 19635.3, state: 'verified', lower_bound: false, reason: null },
      opponent_lane: { ms: 123104.0, state: 'failed', lower_bound: true, reason: null },
      margin_ms: null,
    })

    const roundThree = summariseRounds([bounded])[2]

    expect(roundThree.status).toBe('lakebase_finished')
    expect(roundThree.opponentIsLowerBound).toBe(true)
    // A margin was never measured, so none may be shown.
    expect(roundThree.marginMs).toBeNull()
  })
})

describe('Round 4, which has no opponent to beat', () => {
  it('reads as uncontested rather than a win or a loss', () => {
    const roundFour = summariseRounds([roundFourReceipt()])[3]

    expect(roundFour.status).toBe('uncontested')
    expect(roundFour.lakebaseMs).toBe(12868.36)
    // No opponent is named, because naming one implies somebody lost.
    expect(roundFour.opponent).toBeNull()
    expect(roundFour.opponentMs).toBeNull()
  })

  it('says so on screen instead of leaving the row blank', async () => {
    stubReceipts([roundFourReceipt()])
    render(<Summary onBack={() => {}} />)

    await screen.findByText(/1 of 6 rounds have a result/i)
    const row = document.querySelector('[data-round="put_model_score_in_app"]')

    expect(row).toHaveAttribute('data-status', 'uncontested')
    expect(row).toHaveTextContent(/UNCONTESTED/)
    expect(row).toHaveTextContent(/One lane only/i)
    expect(row).toHaveTextContent('12.87s')
    expect(row).not.toHaveTextContent(/NOT RUN YET|NO RESULT/i)
  })
})

describe('surviving a restart', () => {
  it('renders every completed round with no live session at all', async () => {
    // Exactly the state after a server restart: the session store is empty, so
    // nothing is passed as live, and the board is built from disk alone.
    stubReceipts([receipt(), roundFourReceipt()])
    render(<Summary live={null} onBack={() => {}} />)

    expect(await screen.findByText(/2 of 6 rounds have a result/i)).toBeInTheDocument()
    const board = screen.getByRole('list', { name: /result for each of the six rounds/i })
    expect(within(board).getAllByRole('listitem')).toHaveLength(6)
    expect(document.querySelector('[data-round="wake_idle_app"]'))
      .toHaveTextContent('2.40s')
  })

  it('reads receipts rather than the session store', async () => {
    const fetchMock = stubReceipts([receipt()])
    render(<Summary onBack={() => {}} />)

    await screen.findByText(/1 of 6 rounds have a result/i)
    expect(fetchMock).toHaveBeenCalledWith('/api/receipts', expect.anything())
    expect(fetchMock.mock.calls.some(([url]) => String(url).includes('/api/sessions')))
      .toBe(false)
  })

  it('says so plainly when the record cannot be read', async () => {
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new Error('down')))
    render(<Summary onBack={() => {}} />)

    expect(await screen.findByRole('status')).toHaveTextContent(/could not be reached/i)
  })
})

describe('reading the numbers', () => {
  it('prints seconds under a minute and m:ss above it', () => {
    expect(summaryDuration(2399.8)).toBe('2.40s')
    expect(summaryDuration(59_999)).toBe('60.00s')
    // 480707ms is eight minutes; as a decimal it is unreadable.
    expect(summaryDuration(480_707.65)).toBe('8:00')
    expect(summaryDuration(-1)).toBe('0.00s')
  })

  it('never labels a round with the VERIFIED slogan', () => {
    // Every status, so a new one cannot be added without being checked here.
    const labels = ([
      'running', 'lakebase_faster', 'lakebase_finished', 'uncontested',
      'no_result', 'abandoned', 'unrun',
    ] as const)
      .map((status) => winnerLabel({
        roundId: 'wake_idle_app', roundNumber: '01', roundTitle: 'T', status,
        opponent: null, lakebaseMs: null, opponentMs: null,
        opponentIsLowerBound: false, marginMs: null, source: null, boutsOnRecord: 0,
        stoppedShort: false, sealedAt: null,
      }))

    expect(labels.join(' ')).not.toContain('VERIFIED')
  })
})

describe('verdictFor', () => {
  function result(over: Partial<RoundResult> = {}): RoundResult {
    return {
      roundId: 'wake_idle_app', roundNumber: '01', roundTitle: 'T',
      status: 'lakebase_faster', opponent: 'Aurora Serverless v2',
      lakebaseMs: 2860, opponentMs: 41200, opponentIsLowerBound: false,
      marginMs: 38340, source: 'receipt', boutsOnRecord: 1,
      stoppedShort: false, sealedAt: null, ...over,
    }
  }

  it('never says a round did not run because the record could not be read', () => {
    // The whole reason `unread` exists. Both surfaces score from the record, and
    // a failed read that printed "not run yet" would put six false negatives on
    // a shareable image nobody is standing beside.
    for (const from of [null, result()]) {
      const unread = verdictFor(from, 'unread')
      expect(unread.outcome).toBe('RECORD UNREAD')
      expect(unread.winner).toBeNull()
      expect(unread.figure).toBeNull()
    }
    expect(verdictFor(null, 'reading').outcome).toBe('READING THE RECORD…')
    expect(verdictFor(null, 'read').outcome).toBe('NOT RUN YET')
  })

  it('gives an uncontested round a winner, a figure and no opponent', () => {
    const verdict = verdictFor(result({ status: 'uncontested', opponentMs: null, marginMs: null }), 'read')
    expect(verdict.winner).toEqual({ badge: 'LB', name: 'LAKEBASE' })
    expect(verdict.figure).toBe('2.86s')
    expect(verdict.qualifier).toBe('UNCONTESTED')
    expect(verdict.laneNote).toBe('BLUE CORNER · NO EQUIVALENT NATIVE PATH')
    // No margin, no percentage, no opponent figure: there was no race.
    expect(`${verdict.laneNote} ${verdict.qualifier}`).not.toMatch(/MARGIN|FASTER|%|AURORA/i)
  })

  it('states a stopped round as a lower bound and refuses the margin', () => {
    const verdict = verdictFor(result({
      status: 'lakebase_finished', lakebaseMs: 10_680, opponentMs: 93_997,
      opponentIsLowerBound: true, marginMs: null, stoppedShort: true,
    }), 'read')
    expect(verdict.winner).toEqual({ badge: 'LB', name: 'LAKEBASE' })
    expect(verdict.qualifier).toBe('STOPPED SHORT')
    expect(verdict.laneNote).toBe(
      'AURORA SERVERLESS V2 · UNVERIFIED WHEN STOPPED · LOWER BOUND 1:33 · MARGIN N/A',
    )
    // Their number must never read as a time they achieved.
    expect(verdict.laneNote).toContain('LOWER BOUND')
    expect(verdict.laneNote).toContain('MARGIN N/A')
  })

  it('renders an abandoned round as itself rather than as one never run', () => {
    const abandoned = verdictFor(result({ status: 'abandoned', lakebaseMs: null }), 'read')
    expect(abandoned.outcome).toBe('NO RESULT DECLARED')
    expect(abandoned.qualifier).toBe('ABANDONED')
    expect(abandoned.figure).toBeNull()
    expect(abandoned.outcome).not.toBe(verdictFor(null, 'read').outcome)
  })

  it('gives no round a winner without also giving it a figure', () => {
    // A name beside a blank number reads as a result being withheld.
    const statuses = [
      'running', 'lakebase_faster', 'lakebase_finished', 'uncontested',
      'no_result', 'abandoned', 'unrun',
    ] as const
    for (const status of statuses) {
      const verdict = verdictFor(result({ status }), 'read')
      if (verdict.winner) expect(verdict.figure).not.toBeNull()
      // And never both a winner and a stand-in for one.
      expect(verdict.winner === null || verdict.outcome === null).toBe(true)
    }
  })
})

describe('house rules', () => {
  const css = readFileSync(join(import.meta.dirname, 'summary.css'), 'utf8')

  it('uses no purple', () => {
    expect(css).not.toMatch(/purple|violet|indigo|magenta/i)
  })

  it('stays square and bordered like every other panel', () => {
    expect(css).toContain('border: 3px solid #46527c')
    const radii = [...css.matchAll(/border-radius\s*:\s*([^;}]+)/g)]
      .map(([, value]) => value.trim())
      .filter((value) => !/^0(?:[a-z%]+)?$/.test(value))
    expect(radii).toEqual([])
  })

  it('honours a request for reduced motion', () => {
    expect(css).toContain('@media (prefers-reduced-motion: reduce)')
  })

  it('sets labels in Press Start 2P and prose in weight-900 ui-monospace', () => {
    expect(css).toContain('font-family: "Press Start 2P", monospace')
    const prose = css.slice(css.indexOf('.summary-note'))
    expect(prose).toContain('font-family: ui-monospace, monospace')
    expect(prose).toContain('font-weight: 900')
  })

  it('never prints the VERIFIED slogan anywhere on the screen', async () => {
    stubReceipts([receipt(), roundFourReceipt()])
    const { container } = render(<Summary onBack={() => {}} />)

    await screen.findByText(/2 of 6 rounds have a result/i)
    expect(container.textContent ?? '').not.toContain('VERIFIED')
  })

  it('lets nothing set a width that could overflow a 390px screen', () => {
    // Four overflow bugs in this repo came from a child that could not shrink.
    // Every text box here declares how it wraps and none is given a fixed width.
    // A breakpoint in @media is fine; a fixed width on a box is what traps text.
    expect(css.replace(/@media[^{]+/g, '')).not.toMatch(/[^-]width:\s*\d+px/)
    expect(css).not.toMatch(/white-space:\s*nowrap/)
    expect(css).toContain('overflow-x: hidden')
    const wrapping = [...css.matchAll(/overflow-wrap:\s*anywhere/g)]
    expect(wrapping.length).toBeGreaterThanOrEqual(8)
  })
})

/**
 * Every foreground the summary paints, against the surface it actually sits on.
 *
 * The guard is the pairing, not the colour: #ee452d is legible on --navy and not
 * on a lighter panel, so it is used as a border here and never as text.
 */
describe('contrast', () => {
  const css = readFileSync(join(import.meta.dirname, 'summary.css'), 'utf8')
  const styles = readFileSync(join(import.meta.dirname, 'styles.css'), 'utf8')
  const palette = new Map<string, string>(
    [...styles.matchAll(/(--[\w-]+)\s*:\s*(#[0-9a-fA-F]{3,8})\s*[;}]/g)]
      .map(([, name, value]) => [name, value]),
  )

  function channels(colour: string): [number, number, number] {
    let hex = colour.trim()
    const varied = /^var\(\s*(--[\w-]+)/.exec(hex)
    if (varied) hex = palette.get(varied[1]) ?? ''
    const raw = hex.replace('#', '')
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

  function ratio(foreground: string, background: string) {
    const [hi, lo] = [luminance(channels(foreground)), luminance(channels(background))]
      .sort((a, b) => b - a)
    return (hi + 0.05) / (lo + 0.05)
  }

  const surfaces = ['#02040e', 'var(--navy)', '#151e48']

  it('clears WCAG AA on every foreground the screen can paint', () => {
    const foregrounds = [...new Set(
      [...css.matchAll(/(?:^|[;{])\s*color:\s*([^;}]+)/g)].map(([, value]) => value.trim()),
    )]
    expect(foregrounds.length).toBeGreaterThan(2)

    for (const foreground of foregrounds) {
      for (const surface of surfaces) {
        expect(
          ratio(foreground, surface),
          `${foreground} on ${surface}`,
        ).toBeGreaterThanOrEqual(4.5)
      }
    }
  })

  it('keeps the two accents that fail on a light panel out of the text', () => {
    for (const accent of ['#ee452d', '#f8d83b']) {
      const asText = new RegExp(`color:\\s*${accent}`, 'i')
      if (asText.test(css)) {
        expect(ratio(accent, 'var(--navy)')).toBeGreaterThanOrEqual(4.5)
      }
    }
    expect(css).not.toMatch(/color:\s*#4a83e8/i)
  })
})

describe('the way in', () => {
  const app = readFileSync(join(import.meta.dirname, 'App.tsx'), 'utf8')

  it('is reached by going forward from the end of Round 6', async () => {
    const finale = app.slice(app.indexOf('function Finale('))
    const body = finale.slice(0, finale.indexOf('\nfunction '))
    expect(body).toContain('onClick={onSummary}')
    expect(body).toContain('The rounds you ran')
  })

  it('is routed without needing a session, so a restart cannot strand it', () => {
    expect(app).toContain("if (stage === 'summary')")
    const progress = readFileSync(join(import.meta.dirname, 'progress.ts'), 'utf8')
    const requires = progress.slice(progress.indexOf('export function requiresSession'))
    expect(requires).not.toContain("'summary'")
  })

  it('offers a way back out', async () => {
    const user = userEvent.setup()
    stubReceipts([receipt()])
    const onBack = vi.fn()
    render(<Summary onBack={onBack} />)

    await screen.findByText(/1 of 6 rounds have a result/i)
    await user.click(screen.getByRole('button', { name: /fight card/i }))
    expect(onBack).toHaveBeenCalled()
  })
})
