// What the final card is allowed to say about a round nobody entered against.
//
// WHY THIS FILE EXISTS. Round 6's `comparison_kind` is `capability_gap`: the
// opponent lane reports `not_supported` because no AWS CDC pipeline was built,
// so `competitor_ms` is null and `margin_ms` is null. Every screen that owns
// Round 6 says so in words -- the receipt reads "NOT BUILT OR TIMED · NO HONEST
// TIMER · NO SPEED MARGIN", the round summary reads "One lane only — a time, not
// a race". The final scorecard was the one surface that did not, and it got
// three separate things wrong about the same round:
//
//   1. Its round-number lookup knew Rounds 1-3 only, so Round 6 fell to the
//      row's position in the list and printed whatever index it sat at.
//   2. Its proof label fell through to "Non-executable round · no proof", which
//      is false twice: the round runs, and its proof is the exact order landing
//      in Delta with the count verified.
//   3. It counted a null opponent time as a Lakebase win, so an unraced round
//      arrived in the tally as a "verified win" -- and the staff roll, which
//      says it only restates the card, inherited the same count.
//
// The blank the audience saw was the third of these showing through: a column
// with no number and no reason given reads as a missing measurement.
//
// WHY THESE ASSERTIONS. Each one fails if a specific false string comes back, so
// the copy cannot drift to it again. The tally assertions are the load-bearing
// ones: they distinguish a capability gap from a lane that ran and produced no
// result, which is the distinction the entry could not previously express.

import { cleanup, render, screen, within } from '@testing-library/react'
import { afterEach, describe, expect, it } from 'vitest'
import { FinalScorecard, type ScorecardEntry } from './App'
import { creditsTally, type CreditsScorecardEntry } from './credits-tally'
import type { CreditsEntry } from './credits-entry'
import type { RoundId } from './api/types'

afterEach(cleanup)

const credits: CreditsEntry = {
  competitors: [],
  scorecard: [],
  sound: false,
  boutInFlight: false,
}

/** A row in the shape App.tsx writes to localStorage. */
function row(overrides: Partial<ScorecardEntry> = {}): ScorecardEntry {
  return {
    session_id: 'S1',
    round_id: 'recover_deleted_order' as RoundId,
    round_title: 'Recover this deleted order',
    competitor: 'RDS PostgreSQL',
    lakebase_ms: 15_410,
    competitor_ms: 989_380,
    competitor_censored: false,
    competitor_capability_gap: false,
    remembered_result: 'LAKEBASE RECOVERED THE EXACT ORDER FIRST',
    completed_at: '2026-08-21T15:00:00Z',
    cooldown: null,
    ...overrides,
  }
}

/**
 * A towel thrown before Lakebase itself verified.
 *
 * Neither lane produced a figure, so the round proved nothing -- the state
 * `recap.ts` calls `abandoned`. It is deliberately not the same row as a round
 * nobody selected: an absent row on a card reads as data loss rather than as a
 * decision somebody made.
 */
const ABANDONED = row({
  session_id: 'S3',
  round_id: 'recover_deleted_order' as RoundId,
  lakebase_ms: null,
  competitor_ms: null,
  remembered_result: 'TOWELED AT 45.00s · NO WINNER · MARGIN N/A',
})

/** Round 6 exactly as the server describes it: verified, and never raced. */
const ROUND_SIX = row({
  session_id: 'S6',
  round_id: 'analyze_live_orders_without_slowing_checkout' as RoundId,
  round_title: 'Move live application data into the lakehouse',
  competitor: 'Aurora/RDS',
  lakebase_ms: 1_234,
  competitor_ms: null,
  competitor_capability_gap: true,
  remembered_result: 'LAKEBASE NATIVE CDF WIN · AWS PIPELINE NOT BUILT · MARGIN N/A',
})

const card = (entries: ScorecardEntry[]) => render(
  <FinalScorecard entries={entries} credits={credits} onBack={() => {}} />,
)

describe('the final scorecard on a capability-gap round', () => {
  it('numbers Round 6 by its place in the six, not by its place in the list', () => {
    // Round 6 alone on the card. The old lookup returned null for it and the
    // fallback printed `index + 1`, so this row read "Round 1".
    const { container } = card([ROUND_SIX])
    const only = container.querySelector('.scorecard-row')!
    expect(within(only as HTMLElement).getByText('Round 6')).toBeInTheDocument()
    expect(only).not.toHaveTextContent('Round 1')
  })

  it('numbers every round the same way, whatever order the card is in', () => {
    // The four rounds that can reach this card, deliberately out of order.
    const { container } = card([
      ROUND_SIX,
      row({ session_id: 'A', round_id: 'wake_idle_app' as RoundId }),
      row({ session_id: 'B', round_id: 'make_schema_change_safely' as RoundId }),
      row({ session_id: 'C', round_id: 'recover_deleted_order' as RoundId }),
    ])
    const numbers = [...container.querySelectorAll('.scorecard-row > div > span')]
      .map((node) => node.textContent)
    expect(numbers).toEqual(['Round 6', 'Round 1', 'Round 2', 'Round 3'])
  })

  it('does not call Round 6 a non-executable round with no proof', () => {
    card([ROUND_SIX])
    expect(document.body).not.toHaveTextContent('Non-executable round · no proof')
    expect(screen.getByText('Live order → exact Delta answer')).toBeInTheDocument()
  })

  it('states the absent margin as a capability gap rather than leaving a blank', () => {
    const { container } = card([ROUND_SIX])
    const only = container.querySelector('.scorecard-row')!
    expect(only).toHaveAttribute('data-comparison', 'capability_gap')
    const gap = container.querySelector('.scorecard-gap')
    expect(gap).toBeInTheDocument()
    expect(gap).toHaveTextContent(/capability gap, not a race/i)
    expect(gap).toHaveTextContent(/no margin to report/i)
    // The opponent column says what it is instead of showing a bare N/A beside
    // the opponent's own name, which read as a race whose number went missing.
    expect(only).toHaveTextContent('NOT TIMED')
    expect(only).toHaveTextContent('OPP · NO LANE TO TIME')
  })

  it('leaves a raced round untouched by any of that', () => {
    const { container } = card([row()])
    const only = container.querySelector('.scorecard-row')!
    expect(only).toHaveAttribute('data-comparison', 'raced')
    expect(container.querySelector('.scorecard-gap')).toBeNull()
    expect(only).toHaveTextContent('VS RDS PostgreSQL')
    expect(only).toHaveTextContent('989.38s')
  })

  it('counts an unraced round apart from the wins on screen', () => {
    card([row(), ROUND_SIX])
    // One raced win, one uncontested round. Not "2 verified wins".
    expect(screen.getByText(/1 verified win/)).toBeInTheDocument()
    expect(screen.getByText(/1 uncontested round/)).toBeInTheDocument()
    expect(document.body).not.toHaveTextContent('2 verified wins')
  })

  it('says nothing about uncontested rounds when there are none', () => {
    card([row()])
    expect(screen.getByText(/1 verified win/)).toBeInTheDocument()
    expect(document.body).not.toHaveTextContent(/uncontested/i)
  })
})

describe('the final scorecard on a round somebody stopped', () => {
  it('prints the abandoned round in the ledger\'s own words, with no figure', () => {
    const { container } = card([ABANDONED])
    const only = container.querySelector('.scorecard-row')!

    expect(only).toHaveAttribute('data-comparison', 'abandoned')
    // The same three phrases the finale ledger prints for this state, so a
    // viewer cannot be shown two different accounts of one stopped round.
    expect(only).toHaveTextContent('ABANDONED')
    expect(only).toHaveTextContent('STOPPED BEFORE EITHER LANE VERIFIED')
    expect(only).toHaveTextContent('NO RESULT DECLARED')
    // Nothing was measured, so no lane may carry a number.
    expect(only).not.toHaveTextContent(/\d+\.\d{2}s\s+LB/)
    expect(only).not.toHaveTextContent(/WATCHING/)
  })

  it('does not put an abandoned round in the win count', () => {
    card([row(), ABANDONED])
    expect(screen.getByText(/1 verified win/)).toBeInTheDocument()
    expect(document.body).not.toHaveTextContent('2 verified wins')
    expect(screen.getByText(/1 abandoned round · no result declared/)).toBeInTheDocument()
  })

  it('still gives the abandoned round a row rather than dropping it', () => {
    const { container } = card([row(), ABANDONED])
    expect(container.querySelectorAll('.scorecard-row')).toHaveLength(2)
  })
})

describe('the tally the card and the staff roll share', () => {
  const entry = (over: Partial<CreditsScorecardEntry> = {}): CreditsScorecardEntry => ({
    competitor: 'Aurora',
    lakebase_ms: 900,
    competitor_ms: 4_200,
    ...over,
  })

  it('counts a genuine faster finish as a win', () => {
    expect(creditsTally([entry()])).toMatchObject({ bouts: 1, lakebaseWins: 1, uncontested: 0 })
  })

  it('counts a stopped opponent as a win, because our own lane finished and theirs did not', () => {
    const stopped = entry({ competitor_ms: 90_000, competitor_censored: true })
    expect(creditsTally([stopped])).toMatchObject({ lakebaseWins: 1, uncontested: 0, abandoned: 0 })
  })

  it('does not count a round stopped before our own lane verified as a win', () => {
    /* The towel censors BOTH lanes, so `competitor_censored` is set on a round
       where Lakebase never finished either. Reading that flag alone turned "we
       gave up and they had not finished" into a Lakebase victory -- and a
       campaign that towels every round would have produced four of them.

       `recap.ts` has always refused this: an unverified lane of our own is
       `unproven`, which becomes `abandoned`, which `roundsWithResult` excludes. */
    const stopped = entry({ lakebase_ms: null, competitor_ms: 90_000, competitor_censored: true })
    expect(creditsTally([stopped])).toMatchObject({
      bouts: 1,
      lakebaseWins: 0,
      uncontested: 0,
      abandoned: 1,
    })
  })

  it('does not launder an abandoned round through the uncontested column either', () => {
    // Uncontested means we finished and nobody entered against us. Neither
    // half of that is true here, so it belongs in neither column.
    const nothing = entry({ lakebase_ms: null, competitor_ms: null })
    expect(creditsTally([nothing])).toMatchObject({
      lakebaseWins: 0,
      uncontested: 0,
      abandoned: 1,
    })
  })

  it('does not count a capability gap as a win', () => {
    // The regression proper. This returned lakebaseWins: 1 before.
    const gap = entry({ competitor_ms: null, competitor_capability_gap: true })
    expect(creditsTally([gap])).toMatchObject({ bouts: 1, lakebaseWins: 0, uncontested: 1 })
  })

  it('does not count a slower Lakebase as a win either', () => {
    expect(creditsTally([entry({ lakebase_ms: 5_000 })])).toMatchObject({
      lakebaseWins: 0,
      uncontested: 0,
    })
  })

  it('declines to claim a win on a row written before the gap flag existed', () => {
    // localStorage still holds entries with no `competitor_capability_gap`. A
    // missing opponent time is unattributable on those, so it falls to
    // uncontested: the side that claims less.
    const legacy = entry({ competitor_ms: null })
    expect(creditsTally([legacy])).toMatchObject({ lakebaseWins: 0, uncontested: 1 })
  })

  it('keeps every bout in the bout count regardless of which bucket it lands in', () => {
    const tally = creditsTally([
      entry(),
      entry({ competitor_ms: null, competitor_capability_gap: true }),
      entry({ lakebase_ms: 5_000 }),
    ])
    expect(tally.bouts).toBe(3)
    expect(tally.lakebaseWins + tally.uncontested).toBeLessThanOrEqual(tally.bouts)
  })
})
