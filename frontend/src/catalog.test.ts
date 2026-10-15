import { describe, expect, it } from 'vitest'
import { FALLBACK_CATALOG, recommend, stopCondition } from './catalog'
import { ROUND_FIVE_BELL_PROTOCOL, ROUND_FIVE_MAX_RETRIES } from './round5'

describe('deterministic fight-card recommendation', () => {
  it('selects cold wake for Aurora without changing evidence by persona', () => {
    const sre = recommend(FALLBACK_CATALOG, 'aurora_serverless_v2', ['performance'], 'sre')
    const executive = recommend(FALLBACK_CATALOG, 'aurora_serverless_v2', ['performance'], 'executive')
    expect(sre.round_id).toBe('wake_idle_app')
    expect(executive.round_id).toBe(sre.round_id)
    expect(executive.presenter_opening).not.toBe(sre.presenter_opening)
  })

  it('recommends the executable safe-change round for an RDS developer audience', () => {
    const simple = recommend(FALLBACK_CATALOG, 'rds_postgres', ['simplicity'], 'software_engineer')
    const cost = recommend(FALLBACK_CATALOG, 'rds_postgres', ['cost'], 'software_engineer')
    expect(simple.round_id).toBe('make_schema_change_safely')
    expect(cost.round_id).toBe(simple.round_id)
    expect(cost.metric).not.toBe(simple.metric)
    expect(simple.reason).toContain('Recommended for Software Engineer')
  })

  it('combines multiple priorities without implying measured spend or generic steps', () => {
    const combined = recommend(
      FALLBACK_CATALOG,
      'aurora_serverless_v2',
      ['cost', 'simplicity', 'performance'],
      'sre',
    )
    expect(combined.metric).toBe(
      'Cost inputs, workflow simplicity, and elapsed workflow time to the same verified outcome',
    )
    expect(combined.metric.toLowerCase()).not.toContain('spend')
  })

  it('describes the executable Round 4, Round 5, and Round 6 stop gates', () => {
    expect(stopCondition('put_model_score_in_app')).toMatch(
      /bell cold starts both integrations.*one Delta change.*first application read of the exact row.*250 ms on both lanes/i,
    )
    expect(stopCondition('survive_connection_spike')).toMatch(
      /each pooled-path setup clock stops at an exact application transaction.*included pool.*selected AWS managed pooling path.*new RDS Proxy.*128-attempt, maximum-64-concurrent check.*64-client witness/i,
    )
    expect(stopCondition('analyze_live_orders_without_slowing_checkout')).toMatch(
      /same checkout on both sources.*cold starts AWS DMS and Glue.*first read of the exact order.*own Delta history.*every second on both lanes.*separate checkout/i,
    )
  })

  it('separates login retry budget from the 64-sample no-retry rule', () => {
    const copy = stopCondition('survive_connection_spike', 'aurora_serverless_v2', true)
    expect(copy).toContain(`up to ${ROUND_FIVE_MAX_RETRIES} retried logins`)
    expect(copy).toMatch(/all 64 (verification samples|sampled queries) must succeed with no sample retries/i)
    expect(copy).not.toMatch(/\bzero retries\b/)
    expect(copy).not.toMatch(/64 verification samples per lane with zero retries/)
  })

  it('mirrors server/catalog.py for Round 6: a measured race, AWS cold at the bell', () => {
    const round = FALLBACK_CATALOG.rounds.find((item) => item.id === 'analyze_live_orders_without_slowing_checkout')!
    expect(round.comparison_kind).toBe('measured')
    expect(round.metric_specs?.map((spec) => spec.id)).toEqual([
      'bell_to_exact_history_ms',
      'commit_skew_ms',
      'exact_order_verified',
      'checkout_verified',
    ])
    expect(round.scorecard_by_corner.performance).toBe(
      'Bell to the exact order in the lakehouse, with AWS DMS and Glue cold starting at the bell',
    )
    const claims = round.non_claims?.join(' ') ?? ''
    expect(claims).toMatch(/AWS DMS and Glue cold start at the bell.*change feed is built into the database and always on/i)
    // Only AWS cold starts; nothing calls Lakebase's feed warm, and no AWS stack is "not built".
    expect(claims).not.toMatch(/warm|not built|remains unpriced/i)
  })

  it('keeps the bundled Round 5 catalog on the 10,000-client bell protocol the server runs', () => {
    // The bundle paints this catalog before /api/catalog answers, so it must
    // describe the only Round 5 protocol the server can run (server/catalog.py).
    const round = FALLBACK_CATALOG.rounds.find((item) => item.id === 'survive_connection_spike')!
    expect(round.round5_protocol).toBe(ROUND_FIVE_BELL_PROTOCOL)
    expect(round.metric_specs.find((spec) => spec.role === 'primary')?.id).toBe('bell_to_10000_observed_ms')
    expect(round.scorecard_by_corner.simplicity).toContain(
      'Included Lakebase pooled endpoint versus 3 timed Proxy mutations on the selected AWS managed pooling path',
    )
    expect(round.non_claims).toEqual(expect.arrayContaining([
      expect.stringMatching(/RDS Proxy is the AWS managed pooling option selected.*not a universal Aurora or RDS requirement.*direct connections.*PgBouncer.*application pooling were not compared/i),
      expect.stringMatching(/each independent lane opens exactly 10,000 authenticated retained clients.*holds them for 30 seconds.*9,999 fails/i),
      expect.stringMatching(/PgBouncer product limit is up to 10,000 client connections.*holds exactly that many per lane/i),
    ]))
    expect(round.non_claims?.join(' ')).not.toMatch(
      /128 attempts|maximum 64 concurrent|64-client witness|(?:AWS|Aurora|RDS) (?:requires|required).*RDS Proxy|10,000 (?:backend|simultaneous transaction)|did not (?:load-)?test 10,000|did not exercise that limit/i,
    )
  })
})
