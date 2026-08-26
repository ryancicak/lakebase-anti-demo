import { describe, expect, it } from 'vitest'
import { FALLBACK_CATALOG, recommend, stopCondition } from './catalog'

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
    expect(stopCondition('put_model_score_in_app')).toContain('fresh application connection')
    expect(stopCondition('survive_connection_spike')).toMatch(
      /each clock stops at a verified pooled application path.*RDS Proxy is AWS best practice.*already deployed.*setup delay does not apply.*identical spike.*both lanes/i,
    )
    expect(stopCondition('analyze_live_orders_without_slowing_checkout')).toMatch(
      /exact committed order.*Delta.*separate checkout/i,
    )
  })

  it('keeps the bundled Round 5 catalog aligned to the static-IAM clock boundary', () => {
    const round = FALLBACK_CATALOG.rounds.find((item) => item.id === 'survive_connection_spike')!
    expect(round.scorecard_by_corner.simplicity).toContain(
      'Built-in pooling vs an AWS best-practice RDS Proxy from the declared start',
    )
    expect(round.non_claims).toEqual(expect.arrayContaining([
      expect.stringMatching(/9 journaled competitor mutations.*Proxy security group.*default-egress change.*4 exact security-group rules.*RDS Proxy.*target-group configuration.*target registration/i),
      expect.stringMatching(/IAM service role.*runner permission.*dedicated proxy credential secret.*sealed install-time prerequisites outside the setup clock/i),
    ]))
    expect(round.non_claims?.join(' ')).toMatch(/added RDS Proxy, Secrets Manager, IAM, and network configuration/i)
    expect(round.non_claims?.join(' ')).not.toMatch(/6 setup surfaces|6 handoffs|per-bout (?:proxy )?credential secret/i)
  })
})
