import { describe, expect, it } from 'vitest'

import {
  type BoutReceipt,
  ROUND_ORDER,
  SHARE_THRESHOLD_ROUNDS,
  boutView,
  canShare,
  groupByRound,
  headlineBout,
  roundNumber,
  tally,
} from './recap'

/**
 * Built from real captured sessions: Round 1 against Aurora on 2026-08-20 measured
 * 3.44s against 14.17s with a 0.252ms start gap.
 */
function receipt(overrides: Partial<BoutReceipt> = {}): BoutReceipt {
  return {
    receipt: '61316D86',
    session_id: '61316d86-832f-bc40-1ddc-4c8e0b854fdf',
    round_id: 'wake_idle_app',
    round_title: 'WAKE THIS IDLE APP',
    opponent: 'Aurora Serverless v2',
    opponent_id: 'aurora_serverless_v2',
    outcome: 'declared',
    sealing_event: 'run_finished',
    has_measurements: true,
    metric: 'bout_elapsed_ms',
    lakebase: { ms: 2400.0, state: 'verified', lower_bound: false, reason: null },
    opponent_lane: { ms: 14070.0, state: 'verified', lower_bound: false, reason: null },
    margin_ms: 11670.0,
    start_skew_ms: 0.252,
    sealed_at: '2026-08-15T19:14:32.951Z',
    remembered_result: 'LAKEBASE - 11.66 SECONDS SOONER',
    failure: null,
    ...overrides,
  }
}

describe('boutView', () => {
  it('reads a two-verified-lane bout as timed and keeps its margin', () => {
    const view = boutView(receipt())

    expect(view.kind).toBe('timed')
    expect(view.marginMs).toBe(11670.0)
    expect(view.opponentIsLowerBound).toBe(false)
    expect(view.roundNumber).toBe('01')
    expect(view.startSkewMs).toBe(0.252)
    expect(view.scoreable).toBe(true)
  })

  it('treats an unverified opponent time as a floor and refuses a margin', () => {
    // The Round 2 and Round 3 shape: our lane finished, theirs never verified.
    const view = boutView(
      receipt({
        outcome: 'stopped_short',
        lakebase: { ms: 15310.0, state: 'verified', lower_bound: false, reason: null },
        opponent_lane: {
          ms: 845602.45,
          state: 'failed',
          lower_bound: true,
          reason: 'Could not verify the recovered order',
        },
        margin_ms: null,
        remembered_result: null,
        failure: 'One or more recovered orders could not be verified.',
      }),
    )

    expect(view.kind).toBe('bounded')
    expect(view.opponentIsLowerBound).toBe(true)
    expect(view.marginMs).toBeNull()
  })

  it('never lets a server-sent margin survive an unverified lane', () => {
    // Defence in depth. If the server ever regresses and sends a margin next to a
    // failed lane, the page must still not print it as measured.
    const view = boutView(
      receipt({
        opponent_lane: { ms: 845602.45, state: 'failed', lower_bound: true, reason: 'timed out' },
        margin_ms: 830292.45,
      }),
    )

    expect(view.kind).toBe('bounded')
    expect(view.marginMs).toBeNull()
  })

  it('reads a lane that was never launched as a capability result, not a timing', () => {
    const view = boutView(
      receipt({
        round_id: 'put_model_score_in_app',
        opponent_lane: {
          ms: null,
          state: 'not_supported',
          lower_bound: false,
          reason: 'AWS lane not timed for this Managed Sync proof',
        },
        margin_ms: null,
        start_skew_ms: null,
      }),
    )

    expect(view.kind).toBe('capability')
    expect(view.opponentIsLowerBound).toBe(false)
    expect(view.marginMs).toBeNull()
    // Null is meaningful here: there was no simultaneous start, not a zero one.
    expect(view.startSkewMs).toBeNull()
  })

  it('keeps our own unverified lane visible rather than hiding it', () => {
    const view = boutView(
      receipt({
        outcome: 'stopped_short',
        lakebase: { ms: 4210.0, state: 'failed', lower_bound: true, reason: 'Transaction did not verify' },
        margin_ms: null,
      }),
    )

    expect(view.kind).toBe('unproven')
    expect(view.marginMs).toBeNull()
    expect(view.scoreable).toBe(true)
  })

  it('keeps a thrown towel on the board with both lanes as floors', () => {
    // A towel censors both lanes into lower bounds. Reading only a verified time
    // would drop the bout entirely, which is the outcome the project is proudest
    // of admitting.
    const view = boutView(
      receipt({
        outcome: 'stopped_short',
        sealing_event: 'towel_finished',
        lakebase: { ms: 2110.0, state: 'incomplete', lower_bound: true, reason: 'Stopped' },
        opponent_lane: { ms: 2140.0, state: 'incomplete', lower_bound: true, reason: 'Stopped' },
        margin_ms: null,
        remembered_result: null,
      }),
    )

    expect(view.scoreable).toBe(true)
    expect(view.kind).toBe('unproven')
    expect(view.lakebaseIsLowerBound).toBe(true)
    expect(view.opponentIsLowerBound).toBe(true)
    // Two floors 30ms apart are not a 30ms margin.
    expect(view.marginMs).toBeNull()
  })

  it('marks an attempt that measured nothing as unscoreable', () => {
    const view = boutView(
      receipt({
        outcome: 'stopped_short',
        has_measurements: false,
        lakebase: { ms: null, state: 'failed', lower_bound: false, reason: 'Sealed start state not verified' },
        opponent_lane: { ms: null, state: 'failed', lower_bound: false, reason: null },
        margin_ms: null,
      }),
    )

    expect(view.scoreable).toBe(false)
  })

  it('survives an unparseable timestamp without producing NaN', () => {
    expect(boutView(receipt({ sealed_at: 'not a date' })).sealedAt).toBe(0)
  })
})

describe('groupByRound', () => {
  it('keeps all six rounds so the arc still reads with a partial session', () => {
    const groups = groupByRound([receipt()])

    expect(groups).toHaveLength(6)
    expect(groups.map((group) => group.roundId)).toEqual([...ROUND_ORDER])
    expect(groups[0].unrun).toBe(false)
    expect(groups[0].bouts).toHaveLength(1)
    // The five rounds not run are present and legibly empty, not missing.
    expect(groups.slice(1).every((group) => group.unrun)).toBe(true)
    expect(groups.slice(1).every((group) => group.bouts.length === 0)).toBe(true)
    // An unrun round still carries a title, so the row is not blank.
    expect(groups[1].roundTitle).toBe('MAKE A SCHEMA CHANGE SAFELY')
    expect(groups[1].roundNumber).toBe('02')
  })

  it('gives one row per bout when a round ran against two opponents', () => {
    // Today's session really did this: Round 1 against Aurora and against RDS.
    const groups = groupByRound([
      receipt({ receipt: 'AAAAAAAA', opponent_id: 'aurora_serverless_v2' }),
      receipt({
        receipt: '1847A71A',
        opponent: 'RDS PostgreSQL',
        opponent_id: 'rds_postgres',
        outcome: 'stopped_short',
        opponent_lane: {
          ms: null,
          state: 'not_supported',
          lower_bound: false,
          reason: 'No automatic connection-triggered wake.',
        },
        margin_ms: null,
        start_skew_ms: null,
        sealed_at: '2026-08-15T20:02:00.000Z',
      }),
    ])

    // Neither result is discarded and neither is merged into an invented average.
    expect(groups[0].bouts.map((bout) => bout.receipt.receipt)).toEqual([
      'AAAAAAAA',
      '1847A71A',
    ])
    expect(groups[0].bouts.map((bout) => bout.kind)).toEqual(['timed', 'capability'])
  })

  it('orders a re-run of the same round oldest first', () => {
    const groups = groupByRound([
      receipt({ receipt: 'NEWEST', sealed_at: '2026-08-15T21:00:00.000Z' }),
      receipt({ receipt: 'OLDEST', sealed_at: '2026-08-15T19:00:00.000Z' }),
      receipt({ receipt: 'MIDDLE', sealed_at: '2026-08-15T20:00:00.000Z' }),
    ])

    // A failed attempt followed by a success is a more honest story than only the
    // success, so both are kept and the newest reads as the last row.
    expect(groups[0].bouts.map((bout) => bout.receipt.receipt)).toEqual([
      'OLDEST',
      'MIDDLE',
      'NEWEST',
    ])
  })

  it('renders six empty rows for a session that has run nothing', () => {
    const groups = groupByRound([])

    expect(groups).toHaveLength(6)
    expect(groups.every((group) => group.unrun)).toBe(true)
    expect(groups.every((group) => group.roundTitle.length > 0)).toBe(true)
  })

  it('leaves an unmeasured attempt off the board', () => {
    const groups = groupByRound([receipt({ has_measurements: false, outcome: 'pending' })])

    expect(groups[0].unrun).toBe(true)
  })
})

describe('headlineBout', () => {
  it('has nothing to show for a round that was not run', () => {
    expect(headlineBout([])).toBeNull()
  })

  it('returns the only bout when a round ran once', () => {
    const groups = groupByRound([receipt()])
    expect(headlineBout(groups[0].bouts)?.receipt.receipt).toBe('61316D86')
  })

  it('prefers a bout that carries a real margin over a more recent one', () => {
    const groups = groupByRound([
      receipt({ receipt: 'TIMED', sealed_at: '2026-08-15T19:00:00.000Z' }),
      receipt({
        receipt: 'CAPABILITY',
        sealed_at: '2026-08-15T21:00:00.000Z',
        opponent_lane: { ms: null, state: 'not_supported', lower_bound: false, reason: 'no lane' },
        margin_ms: null,
        start_skew_ms: null,
      }),
    ])

    expect(headlineBout(groups[0].bouts)?.receipt.receipt).toBe('TIMED')
  })

  it('falls back to the most recent when no bout carries a margin', () => {
    const groups = groupByRound([
      receipt({
        receipt: 'EARLIER',
        sealed_at: '2026-08-15T19:00:00.000Z',
        opponent_lane: { ms: 900.0, state: 'failed', lower_bound: true, reason: 'timed out' },
        margin_ms: null,
      }),
      receipt({
        receipt: 'LATER',
        sealed_at: '2026-08-15T21:00:00.000Z',
        opponent_lane: { ms: 900.0, state: 'failed', lower_bound: true, reason: 'timed out' },
        margin_ms: null,
      }),
    ])

    expect(headlineBout(groups[0].bouts)?.receipt.receipt).toBe('LATER')
  })
})

describe('tally', () => {
  it('reports an empty session without poisoning the fairness line', () => {
    const summary = tally([])

    expect(summary.empty).toBe(true)
    expect(summary.boutsRun).toBe(0)
    expect(summary.roundsRun).toBe(0)
    expect(summary.roundsDeclared).toBe(0)
    expect(summary.roundsTotal).toBe(6)
    // The bug this pins: Math.max() over an empty list returns -Infinity, which
    // would render as "WORST START GAP -InfinityMS".
    expect(summary.worstSkewMs).toBeNull()
    expect(Number.isFinite(summary.worstSkewMs as number)).toBe(false)
    expect(summary.boutsWithoutSharedStart).toBe(0)
  })

  it('counts a single bout as a legitimate one-round session', () => {
    const summary = tally([receipt()])

    expect(summary.empty).toBe(false)
    expect(summary.boutsRun).toBe(1)
    expect(summary.roundsRun).toBe(1)
    expect(summary.declared).toBe(1)
    expect(summary.roundsDeclared).toBe(1)
    expect(summary.worstSkewMs).toBe(0.252)
  })

  it('counts bouts and rounds separately when a round ran twice', () => {
    const summary = tally([
      receipt({ receipt: 'A', start_skew_ms: 0.252 }),
      receipt({ receipt: 'B', start_skew_ms: 0.741, sealed_at: '2026-08-15T20:00:00.000Z' }),
    ])

    // Two bouts, one round. Reporting "2 of 6 rounds" here would be a miscount.
    expect(summary.boutsRun).toBe(2)
    expect(summary.roundsRun).toBe(1)
    expect(summary.roundsDeclared).toBe(1)
    expect(summary.worstSkewMs).toBe(0.741)
  })

  it('reports the worst skew across only the bouts that had a shared start', () => {
    const summary = tally([
      receipt({ receipt: 'TIMED', start_skew_ms: 0.318 }),
      receipt({
        receipt: 'CAPABILITY',
        round_id: 'put_model_score_in_app',
        opponent_lane: { ms: null, state: 'not_supported', lower_bound: false, reason: 'no lane' },
        margin_ms: null,
        start_skew_ms: null,
      }),
    ])

    expect(summary.worstSkewMs).toBe(0.318)
    expect(summary.boutsWithoutSharedStart).toBe(1)
    expect(summary.roundsRun).toBe(2)
  })

  it('ignores a start gap reported for a lane that never started', () => {
    // Observed from the harness: a bout against an ineligible opponent came back
    // with launch_skew_ms 0.0 and no opponent timing. Averaging that in would
    // advertise a perfect simultaneous start that never happened.
    const summary = tally([
      receipt({ receipt: 'TIMED', start_skew_ms: 0.318 }),
      receipt({
        receipt: 'NEVER_STARTED',
        round_id: 'make_schema_change_safely',
        start_skew_ms: 0.0,
        opponent_lane: {
          ms: null,
          state: 'not_supported',
          lower_bound: false,
          reason: 'No automatic connection-triggered wake.',
        },
        margin_ms: null,
      }),
    ])

    expect(summary.worstSkewMs).toBe(0.318)
    expect(summary.boutsWithoutSharedStart).toBe(1)
  })

  it('reports a null worst skew when no bout had a shared start at all', () => {
    const summary = tally([
      receipt({
        opponent_lane: { ms: null, state: 'not_supported', lower_bound: false, reason: 'no lane' },
        margin_ms: null,
        start_skew_ms: null,
      }),
    ])

    expect(summary.empty).toBe(false)
    expect(summary.worstSkewMs).toBeNull()
    expect(summary.boutsWithoutSharedStart).toBe(1)
  })

  it('separates declared from stopped short', () => {
    const summary = tally([
      receipt({ receipt: 'DECLARED' }),
      receipt({
        receipt: 'STOPPED',
        round_id: 'recover_deleted_order',
        outcome: 'stopped_short',
        opponent_lane: { ms: 310842.36, state: 'failed', lower_bound: true, reason: 'not verified' },
        margin_ms: null,
        remembered_result: null,
      }),
    ])

    expect(summary.declared).toBe(1)
    expect(summary.stoppedShort).toBe(1)
    expect(summary.roundsRun).toBe(2)
    // A stopped-short round is not a declared one, so the numerator stays honest.
    expect(summary.roundsDeclared).toBe(1)
  })

  it('excludes attempts that measured nothing from every count', () => {
    const summary = tally([receipt({ has_measurements: false, outcome: 'pending' })])

    expect(summary.empty).toBe(true)
    expect(summary.boutsRun).toBe(0)
  })
})

describe('canShare', () => {
  it('withholds the share action for an empty session', () => {
    expect(canShare(tally([]))).toBe(false)
  })

  it('withholds it for a thin session that would share badly', () => {
    // A legitimate page, but a card with four of six cells empty.
    const thin = tally([
      receipt({ receipt: 'A' }),
      receipt({ receipt: 'B', round_id: 'make_schema_change_safely' }),
    ])

    expect(thin.roundsDeclared).toBe(2)
    expect(canShare(thin)).toBe(false)
  })

  it('offers it once enough rounds have been declared', () => {
    const enough = tally(
      ROUND_ORDER.slice(0, SHARE_THRESHOLD_ROUNDS).map((round_id, index) =>
        receipt({ receipt: `R${index}`, round_id }),
      ),
    )

    expect(canShare(enough)).toBe(true)
  })
})

describe('roundNumber', () => {
  it('numbers every round in the catalog and refuses to guess at an unknown one', () => {
    expect(ROUND_ORDER.map(roundNumber)).toEqual(['01', '02', '03', '04', '05', '06'])
    // @ts-expect-error deliberately off-catalog: a future round id must not render as "01".
    expect(roundNumber('a_round_that_does_not_exist')).toBe('--')
  })
})
