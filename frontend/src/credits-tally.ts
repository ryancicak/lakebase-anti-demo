// What the credits roll is allowed to say about the bouts, and the one
// function that works it out.
//
// Split out of credits.tsx so that file exports components only: a module that
// mixes component and non-component exports breaks Vite's fast refresh, which
// is what react-refresh/only-export-components is guarding.

import { resultKind } from './recap'
import type { ContractStatus, FormalWinner } from './outcome'

/**
 * Structural subset of App.tsx's ScorecardEntry.
 *
 * Declared here rather than imported so the dependency runs one way only:
 * App.tsx -> credits-entry.tsx -> credits-tally.ts. Importing the real
 * interface back out of App.tsx would close that into a cycle for the sake of
 * four fields, and App's own entries satisfy this shape structurally.
 */
export interface CreditsScorecardEntry {
  competitor: string
  /** Null when our own lane never verified, which is what an early towel leaves. */
  lakebase_ms: number | null
  competitor_ms: number | null
  competitor_censored?: boolean
  /** True when the opponent lane reported `not_supported` -- nothing was timed. */
  competitor_capability_gap?: boolean
  contract_status?: ContractStatus
  formal_winner?: FormalWinner
}

export interface CreditsTally {
  bouts: number
  lakebaseWins: number
  /** Bouts with no opponent time to compare against, counted apart from the wins. */
  uncontested: number
  /** Optional for callers constructing the pre-classifier tally shape. */
  incomplete?: number
  /**
   * Bouts stopped before our own lane verified. Neither a win nor uncontested:
   * uncontested means we finished and nobody entered against us, and only the
   * second half of that is true here.
   */
  abandoned: number
  competitors: string[]
}

/**
 * The roll never derives a result of its own; it restates what the scorecard
 * already verified -- which means it has to split the same way the scorecard
 * splits.
 *
 * Two things this has been got wrong about, both fixed by asking `recap.ts`
 * rather than answering here:
 *
 * 1. A missing opponent time counted as a win, so Round 6 -- a capability gap
 *    with no AWS lane to time -- arrived in the roll as a "verified win". A
 *    round nobody entered against cannot be won.
 * 2. `competitor_censored` counted as a win on its own. The towel censors BOTH
 *    lanes, so that flag is set on rounds where Lakebase never finished either,
 *    and reading it alone turned "we gave up and they had not finished" into a
 *    victory. `resultKind` asks about our own lane first, for exactly this.
 *
 * The roll's own closing line is "Where no fair margin existed, none was
 * claimed."
 */
export function creditsTally(entries: CreditsScorecardEntry[]): CreditsTally {
  let lakebaseWins = 0
  let uncontested = 0
  let incomplete = 0
  let abandoned = 0
  for (const entry of entries) {
    if (entry.contract_status !== undefined) {
      if (entry.contract_status === 'declared_capability') {
        uncontested += 1
      } else if (
        entry.formal_winner === 'lakebase'
        && (
          entry.contract_status === 'declared_comparison'
          || entry.contract_status === 'adjudicated_stoppage'
          || entry.contract_status === 'cleanup_failure'
        )
      ) {
        lakebaseWins += 1
      } else if (
        entry.contract_status === 'comparison_incomplete'
        || entry.contract_status === 'guardrail_failure'
        || entry.contract_status === 'cleanup_failure'
      ) {
        incomplete += 1
      } else if (entry.contract_status === 'no_verified_evidence') {
        abandoned += 1
      }
      continue
    }
    const lakebaseMs = entry.lakebase_ms
    const competitorMs = entry.competitor_ms
    // A lane that was never built is not a bounded one: nothing ran to bound.
    // A row written before the gap flag existed carries a null time it cannot
    // attribute, so it falls here too -- the side that claims less.
    const opponentTimed = !entry.competitor_capability_gap && competitorMs !== null
    const kind = resultKind({
      lakebaseVerified: lakebaseMs !== null,
      opponentVerified: opponentTimed && !entry.competitor_censored,
      opponentBounded: opponentTimed && entry.competitor_censored === true,
    })
    switch (kind) {
      case 'unproven':
        abandoned += 1
        break
      case 'capability':
        uncontested += 1
        break
      case 'bounded':
        // Ours finished, theirs did not. No margin, but a winner.
        lakebaseWins += 1
        break
      default:
        // Both lanes verified, so the two figures are comparable.
        if (lakebaseMs !== null && competitorMs !== null && lakebaseMs < competitorMs) lakebaseWins += 1
    }
  }
  return {
    bouts: entries.length,
    lakebaseWins,
    uncontested,
    incomplete,
    abandoned,
    competitors: [...new Set(entries.map((entry) => entry.competitor))],
  }
}
