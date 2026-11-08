import { describe, expect, it } from 'vitest'

import type { CustomerCorner, PersonaId } from './api/types'
import {
  getLensLine,
  getResultLine,
  getRoleLine,
  LENS_LINE_RECORDS,
  PERSONA_IDS,
  PRIORITIES,
  RESULT_LINE_RECORDS,
  ROLE_LINE_RECORDS,
  ROUND_FIVE_PERSONA_MEANING,
  ROUND_IDS,
} from './ringside-cues/corpus'
import { OUTCOME_IDS } from './ringside-cues/types'
import { roundFiveFightCardOpening } from './round5'

/** AWS's products, and the words Rounds 4 to 6 use for AWS's stack without naming AWS. */
const AWS_WORDS = /\b(AWS|Aurora|RDS|Glue|DMS|Proxy|S3|IAM|Secrets Manager|replication slot|write-ahead log)\b/
/** Azure's products, which the AWS edition's lines must never name. */
const AZURE_WORDS = /\b(Azure|PgBouncer|Data Factory|data factory|data flow|ADLS|General Purpose|Memory Optimized)\b/i

describe('the Azure edition reads the same briefing with Azure’s stack named', () => {
  it('has a variant only where the shared line is about AWS’s stack, and the variant names none of it', () => {
    const variants = [
      ...RESULT_LINE_RECORDS.map((record) => ({ scope: record.competitor, text: record.text, key: `${record.round_id}/${record.outcome_id}` })),
      ...ROLE_LINE_RECORDS.map((record) => ({ scope: record.competitor, text: `${record.why} ${record.ask}`, key: `${record.round_id}/${record.persona_id}` })),
      ...LENS_LINE_RECORDS.map((record) => ({ scope: record.competitor, text: record.text, key: `${record.round_id}/${record.priority}` })),
    ]
    const shared = new Map(variants.filter((line) => line.scope === '*').map((line) => [line.key, line.text]))
    const azure = variants.filter((line) => line.scope === 'azure')
    expect(azure.length).toBeGreaterThan(0)
    for (const line of azure) {
      const original = shared.get(line.key)
      expect(original, line.key).toBeDefined()
      // A variant says something different about the other side's stack: Round 1's
      // shared lines are about Aurora's floor and wake, Round 4's about a Glue job
      // ("a job you build and babysit"), Rounds 5 and 6's about RDS Proxy, DMS and Glue.
      expect(line.text, line.key).not.toBe(original)
      expect(line.text, line.key).not.toMatch(AWS_WORDS)
    }
    for (const line of variants.filter((candidate) => candidate.scope !== 'azure')) {
      expect(line.text, line.key).not.toMatch(AZURE_WORDS)
    }
  })

  it('never shows an Azure bout an AWS product, in any round, role, priority, or outcome', () => {
    for (const roundId of ROUND_IDS) {
      for (const personaId of PERSONA_IDS as readonly PersonaId[]) {
        const role = getRoleLine(roundId, personaId, 'azure_postgres')
        expect(`${role.why} ${role.ask}`, `${roundId}/${personaId}`).not.toMatch(AWS_WORDS)
      }
      for (const priority of PRIORITIES as readonly CustomerCorner[]) {
        expect(getLensLine(roundId, priority, 'azure_postgres').text, `${roundId}/${priority}`).not.toMatch(AWS_WORDS)
      }
      for (const outcomeId of OUTCOME_IDS) {
        const shared = RESULT_LINE_RECORDS.find(
          (record) => record.round_id === roundId && record.outcome_id === outcomeId && record.competitor === '*',
        )
        if (!shared) continue
        expect(getResultLine(roundId, outcomeId, 'azure_postgres').text, `${roundId}/${outcomeId}`).not.toMatch(AWS_WORDS)
      }
    }
  })

  it('reaches an Azure bout alone', () => {
    const azure = getLensLine('survive_connection_spike', 'cost', 'azure_postgres')
    expect(azure.competitor).toBe('azure')
    expect(getLensLine('survive_connection_spike', 'cost', 'rds_postgres').competitor).toBe('*')
    expect(getLensLine('survive_connection_spike', 'cost', 'aurora_serverless_v2').competitor).toBe('*')
    expect(getLensLine('survive_connection_spike', 'cost').competitor).toBe('*')
  })

  it('gives RDS its own Round 1 cost and simplicity lines, since RDS never sleeps', () => {
    for (const priority of ['cost', 'simplicity'] as const) {
      const rds = getLensLine('wake_idle_app', priority, 'rds_postgres')
      expect(rds.competitor).toBe('rds')
      expect(rds.text).toMatch(/\bRDS\b/)
      expect(rds.text).not.toMatch(/Aurora/)
      expect(getLensLine('wake_idle_app', priority, 'aurora_serverless_v2').text).toMatch(/Aurora|woke on the app's own connection/)
    }
  })

  it('opens the Azure fight card in Azure’s words, with the AWS edition’s shape', () => {
    const openings = PERSONA_IDS.map((personaId) =>
      roundFiveFightCardOpening(personaId, 'azure_postgres'))
    expect(new Set(openings).size).toBe(PERSONA_IDS.length)
    for (const opening of openings) {
      expect(opening).not.toMatch(AWS_WORDS)
      expect(opening).toMatch(/\bup to 10,000 client connections\b/i)
      expect(opening).not.toMatch(
        /[–—]|\b(?:spike|survive|witness|scheduled clients?|terminal clients?|setup stop|contract|128 attempts?|maximum 64 concurrent)\b/i,
      )
      expect(opening.split(/\s+/).length).toBeLessThanOrEqual(34)
    }
    // The AWS edition's openings are its own, untouched.
    for (const personaId of PERSONA_IDS) {
      expect(roundFiveFightCardOpening(personaId, 'rds_postgres'))
        .toBe(ROUND_FIVE_PERSONA_MEANING[personaId])
      expect(roundFiveFightCardOpening(personaId)).toBe(ROUND_FIVE_PERSONA_MEANING[personaId])
    }
  })
})
