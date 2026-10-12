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

import { act, cleanup, render, screen, waitFor, within } from '@testing-library/react'
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
import { finaleLaneBars } from './App'
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

    await waitFor(() => expect(screen.getByRole('status')).toHaveTextContent(/1 of 6 rounds have a result/i))
    expect(fetchMock).toHaveBeenCalledWith('/api/receipts', expect.anything())
    expect(fetchMock.mock.calls.some(([url]) => String(url).includes('/api/sessions')))
      .toBe(false)
  })

  it('counts an empty record as read, with every round not run yet', async () => {
    stubReceipts([])
    render(<Summary onBack={() => {}} />)

    await within(screen.getByRole('status')).findByText('0 of 6 rounds have a result.')
    const board = screen.getByRole('list', { name: /result for each of the six rounds/i })
    expect(within(board).getAllByRole('listitem')).toHaveLength(6)
    expect(within(board).getAllByText('NOT RUN YET')).toHaveLength(6)
  })

  function expectBoardRows(text: string) {
    const board = screen.getByRole('list', { name: /result for each of the six rounds/i })
    const rows = within(board).getAllByRole('listitem')
    expect(rows).toHaveLength(6)
    for (const row of rows) {
      expect(row).toHaveTextContent(text)
      expect(row).not.toHaveTextContent('NOT RUN YET')
    }
  }

  // A failed read is not an empty record.
  async function expectUnreadBoard() {
    await within(screen.getByRole('status')).findByText('Record unread · no count until it can be read.')
    expectBoardRows('RECORD UNREAD')
    expect(screen.queryByText(/0 of 6 rounds have a result/i)).not.toBeInTheDocument()
    expect(screen.queryByText(/rounds have a result/i)).not.toBeInTheDocument()
  }

  it('says so plainly when the record cannot be read', async () => {
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new Error('down')))
    render(<Summary onBack={() => {}} />)

    const detail = await screen.findByText('The record could not be reached. Select Refresh to try again.')
    // One live region: the detail is read out as part of the record line.
    expect(screen.getByRole('status')).toContainElement(detail)
    expect(document.querySelectorAll('[role="status"], [aria-live]')).toHaveLength(1)
  })

  it('tells the operator to Refresh after a read that timed out', async () => {
    vi.useFakeTimers()
    try {
      vi.stubGlobal('fetch', vi.fn().mockImplementation((_url: string, init?: RequestInit) => (
        new Promise((_resolve, reject) => {
          init?.signal?.addEventListener('abort', () => reject(new DOMException('aborted', 'AbortError')))
        })
      )))
      render(<Summary onBack={() => {}} />)
      // The copy says 15 s, so the read must still be pending a millisecond short of that.
      await act(async () => { await vi.advanceTimersByTimeAsync(14_999) })
      expect(screen.getByRole('status')).toHaveTextContent('Reading the record…')
      await act(async () => { await vi.advanceTimersByTimeAsync(1) })
    } finally {
      vi.useRealTimers()
    }
    const status = screen.getByRole('status')
    await within(status).findByText('The record did not answer within 15 s. Select Refresh to try again.')
    expect(status).not.toHaveTextContent(/could not be reached|reconnect automatically/i)
    await expectUnreadBoard()
  })

  it('never prints NOT RUN YET or a zero count when the read is rejected', async () => {
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new Error('down')))
    render(<Summary onBack={() => {}} />)
    await expectUnreadBoard()
  })

  it('never prints NOT RUN YET or a zero count when the read returns 503', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({
      ok: false,
      status: 503,
      statusText: 'Service Unavailable',
      json: () => Promise.resolve({}),
    }))
    render(<Summary onBack={() => {}} />)
    await expectUnreadBoard()
    // A server answer keeps its own text; only a lost connection is told to Refresh.
    expect(screen.getByRole('status')).toHaveTextContent('503 Service Unavailable')
    expect(screen.getByRole('status')).not.toHaveTextContent(/select refresh to try again/i)
  })

  it('never prints NOT RUN YET while the record is still being read', async () => {
    vi.stubGlobal('fetch', vi.fn().mockReturnValue(new Promise(() => {})))
    render(<Summary onBack={() => {}} />)
    expect(await screen.findByRole('status')).toHaveTextContent('Reading the record…')
    expectBoardRows('READING THE RECORD…')
  })

  it('goes back to reading when Refresh follows a failed read', async () => {
    const fetchMock = vi.fn()
      .mockRejectedValueOnce(new Error('down'))
      .mockReturnValueOnce(new Promise(() => {}))
    vi.stubGlobal('fetch', fetchMock)
    const user = userEvent.setup()
    render(<Summary onBack={() => {}} />)
    await expectUnreadBoard()

    const status = screen.getByRole('status')
    await user.click(screen.getByRole('button', { name: /refresh/i }))
    expect(await screen.findByRole('status')).toHaveTextContent('Reading the record…')
    expect(screen.getByRole('status')).toBe(status)
    expect(screen.queryByText(/could not be reached/i)).not.toBeInTheDocument()
    expectBoardRows('READING THE RECORD…')
    expect(fetchMock).toHaveBeenCalledTimes(2)
  })

  it('keeps no earlier receipt on the board when Refresh after a good read fails', async () => {
    let failRefresh = () => {}
    const fetchMock = vi.fn()
      .mockResolvedValueOnce({
        ok: true,
        status: 200,
        statusText: 'OK',
        json: () => Promise.resolve({ receipts: [receipt()] }),
      })
      .mockReturnValueOnce(new Promise((_resolve, reject) => {
        failRefresh = () => reject(new Error('down'))
      }))
    vi.stubGlobal('fetch', fetchMock)
    const user = userEvent.setup()
    render(<Summary onBack={() => {}} />)
    await within(screen.getByRole('status')).findByText('1 of 6 rounds have a result.')
    expect(document.querySelector('[data-round="wake_idle_app"]')).toHaveTextContent('2.40s')

    await user.click(screen.getByRole('button', { name: /refresh/i }))
    // The earlier answer is not the record any more once Refresh is reading it again.
    expect(screen.getByRole('status')).toHaveTextContent('Reading the record…')
    expectBoardRows('READING THE RECORD…')
    expect(screen.queryByText('2.40s')).not.toBeInTheDocument()

    await act(async () => { failRefresh() })
    await expectUnreadBoard()
    expect(screen.queryByText('2.40s')).not.toBeInTheDocument()
    expect(screen.queryByText('LAKEBASE')).not.toBeInTheDocument()
    expect(fetchMock).toHaveBeenCalledTimes(2)
  })

  it.each(['fails', 'answers'])('ignores a read that %s after Refresh has replaced it', async (ending) => {
    let settleFirst = () => {}
    const fetchMock = vi.fn()
      .mockReturnValueOnce(new Promise((resolve, reject) => {
        settleFirst = ending === 'fails'
          ? () => reject(new Error('down'))
          : () => resolve({
            ok: true,
            status: 200,
            statusText: 'OK',
            json: () => Promise.resolve({ receipts: [receipt()] }),
          })
      }))
      .mockReturnValueOnce(new Promise(() => {}))
    vi.stubGlobal('fetch', fetchMock)
    const user = userEvent.setup()
    render(<Summary onBack={() => {}} />)

    await user.click(screen.getByRole('button', { name: /refresh/i }))
    expect(fetchMock).toHaveBeenCalledTimes(2)
    await act(async () => {
      settleFirst()
      await new Promise((resolve) => setTimeout(resolve, 0))
    })

    expect(screen.getByRole('status')).toHaveTextContent('Reading the record…')
    expectBoardRows('READING THE RECORD…')
  })

  it('still marks a live round as running when the record is unread', async () => {
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new Error('down')))
    render(<Summary live={{ roundId: 'wake_idle_app', running: true }} onBack={() => {}} />)
    await waitFor(() => expect(screen.getByRole('status')).toHaveTextContent(/record unread/i))
    const row = document.querySelector('[data-round="wake_idle_app"]')
    expect(row).toHaveTextContent('RUNNING NOW')
    expect(row).not.toHaveTextContent('NOT RUN YET')
  })
})

describe('reading the numbers', () => {
  it('prints seconds under a minute and minutes above it, each with its unit', () => {
    expect(summaryDuration(2399.8)).toBe('2.40s')
    expect(summaryDuration(59_999)).toBe('60.00s')
    // 480707ms is eight minutes; as a decimal it is unreadable.
    expect(summaryDuration(480_707.65)).toBe('8m 00s')
    // A bare 1:48 beside Lakebase's 11.33s read as the faster lane.
    expect(summaryDuration(108_210)).toBe('1m 48s')
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
        opponent: null, opponentId: null, lakebaseMs: null, opponentMs: null, lakebaseIsLowerBound: false,
        opponentIsLowerBound: false, marginMs: null, source: null, boutsOnRecord: 0,
        stoppedShort: false, towelThrown: false, sealedAt: null,
      }))

    expect(labels.join(' ')).not.toContain('VERIFIED')
  })
})

/**
 * Ryan's rule for every round: at a towel each lane shows where its clock stood,
 * marked unfinished, and never as a finish time.
 */
describe('a towel on the ledger', () => {
  // Receipt 3C275636 on the v1.1 test app: Round 6 toweled 1.02 s after its bell,
  // both clocks unfinished at the same floor. The summary printed "1.02s vs >1.02s".
  const floor: BoutReceipt['lakebase'] = {
    ms: 1023.598057,
    state: 'incomplete',
    lower_bound: true,
    reason: 'Toweled · unfinished at cutoff · >1.02s lower bound',
  }
  const towelled = receipt({
    receipt: '3C275636',
    session_id: '3c275636899549549505ce99c1ef02de',
    round_id: 'analyze_live_orders_without_slowing_checkout',
    round_title: 'Move live application data into the lakehouse',
    opponent: 'RDS PostgreSQL',
    opponent_id: 'rds_postgres',
    outcome: 'stopped_short',
    sealing_event: 'towel_finished',
    lakebase: floor,
    opponent_lane: floor,
    margin_ms: null,
    start_skew_ms: null,
    sealed_at: '2026-09-30T02:02:42.418943Z',
    remembered_result: 'Toweled at >1.02s · No exact result verified · No winner · Margin N/A',
  })
  const roundSix = () => summariseRounds([towelled])
    .find((result) => result.roundId === 'analyze_live_orders_without_slowing_checkout')!

  it('carries each lane as a floor, never a finish time', () => {
    expect(roundSix()).toMatchObject({
      status: 'no_result',
      lakebaseMs: 1023.598057,
      opponentMs: 1023.598057,
      lakebaseIsLowerBound: true,
      opponentIsLowerBound: true,
    })
  })

  it('prints both floors on the summary, each marked, and says why', async () => {
    stubReceipts([towelled])
    render(<Summary onBack={() => {}} />)
    await screen.findByText(/of 6 rounds have a result/i)

    const row = document.querySelector('[data-round="analyze_live_orders_without_slowing_checkout"]')!
    expect(row.querySelector('.summary-time')).toHaveTextContent('>1.02s')
    expect(row.querySelector('.summary-against')).toHaveTextContent('vs >1.02s RDS PostgreSQL')
    expect(row.textContent).not.toMatch(/(^|[^>])1\.02s/)
    expect(row).toHaveTextContent(/Towel thrown/)
  })

  it('names the towel for a towel whose cleanup did not settle cleanly', async () => {
    stubReceipts([{ ...towelled, sealing_event: 'towel_update' }])
    render(<Summary onBack={() => {}} />)
    await screen.findByText(/of 6 rounds have a result/i)
    const row = document.querySelector('[data-round="analyze_live_orders_without_slowing_checkout"]')!
    expect(row).toHaveTextContent('Towel thrown · > marks where an unfinished clock stood.')
  })

  it('does not claim a towel for a floor nobody threw one for', async () => {
    // A Round 4 or 6 lane stopped at its bound, a finish past the bound, and a lost
    // ring all seal a floor on events no towel is involved in.
    for (const sealing_event of ['session_failed', 'run_finished', 'cleanup_update']) {
      stubReceipts([{ ...towelled, sealing_event }])
      const { unmount } = render(<Summary onBack={() => {}} />)
      await screen.findByText(/of 6 rounds have a result/i)
      const row = document.querySelector('[data-round="analyze_live_orders_without_slowing_checkout"]')!
      expect(row, sealing_event).not.toHaveTextContent(/towel/i)
      expect(row, sealing_event).toHaveTextContent('> marks where an unfinished clock stood.')
      // The floors themselves are unchanged: still marked, still never a finish time.
      expect(row.querySelector('.summary-time'), sealing_event).toHaveTextContent('>1.02s')
      unmount()
    }
  })

  it('draws both floors on the finale tile, with no proportional fill', () => {
    const bars = finaleLaneBars(roundSix())
    expect(bars.lakebase).toEqual({ share: null, label: '>1.02s', lowerBound: true })
    expect(bars.opponent).toEqual({ share: null, label: '>1.02s', lowerBound: true })
  })

  it('never draws a floor as a finish time, whichever lane holds it', () => {
    // The blue corner verified first and the towel stopped Lakebase's clock at 4 s.
    const bars = finaleLaneBars({
      ...roundSix(),
      status: 'competitor_faster',
      lakebaseMs: 4000,
      opponentMs: 3000,
      lakebaseIsLowerBound: true,
      opponentIsLowerBound: false,
    })
    expect(bars.lakebase).toEqual({ share: null, label: '>4.00s', lowerBound: true })
    expect(bars.opponent).toEqual({ share: null, label: '3.00s' })
  })
})

describe('verdictFor', () => {
  function result(over: Partial<RoundResult> = {}): RoundResult {
    return {
      roundId: 'wake_idle_app', roundNumber: '01', roundTitle: 'T',
      status: 'lakebase_faster', opponent: 'Aurora Serverless v2', opponentId: 'aurora_serverless_v2',
      lakebaseMs: 2860, opponentMs: 41200, lakebaseIsLowerBound: false, opponentIsLowerBound: false,
      marginMs: 38340, source: 'receipt', boutsOnRecord: 1,
      stoppedShort: false, towelThrown: false, sealedAt: null, ...over,
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
      'AURORA SERVERLESS V2 · UNVERIFIED WHEN STOPPED · LOWER BOUND 1m 33s · MARGIN N/A',
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

  it('wraps the failure line by its own class, whatever element carries it', () => {
    // Not `span.summary-status`: a rule qualified by its element stops applying when the element changes.
    const status = /(?<![\w-])\.summary-status\s*\{([^}]*)\}/.exec(css)
    expect(status?.[1]).toMatch(/overflow-wrap:\s*anywhere/)
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
