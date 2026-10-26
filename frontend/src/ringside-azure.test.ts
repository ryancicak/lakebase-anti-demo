import { createHash } from 'node:crypto'
import { readFileSync } from 'node:fs'
import { join } from 'node:path'

import { describe, expect, it } from 'vitest'

import {
  getOutcomeRecord,
  getRoundFivePersonaOutcomeRecord,
  getVerifiedRecord,
  OUTCOME_COPY_AZURE_RECORDS,
  OUTCOME_COPY_AZURE_SHA256,
  OUTCOME_COPY_RECORDS,
  PERSONA_IDS,
  ROUND_FIVE_PERSONA_MEANING,
  ROUND_FIVE_PERSONA_OUTCOME_AZURE_RECORDS,
  ROUND_FIVE_PERSONA_OUTCOME_RECORDS,
  ROUND_FIVE_PERSONA_OUTCOMES_AZURE_SHA256,
  VERIFIED_CORPUS_AZURE_RECORDS,
  VERIFIED_CORPUS_AZURE_SHA256,
  VERIFIED_CORPUS_RECORDS,
} from './ringside-cues/corpus'
import { PRIORITY_KEYS } from './ringside-cues/types'
import { roundFiveFightCardOpening } from './round5'

const AWS_WORDS = /\b(AWS|Aurora|RDS|Glue|DMS)\b/
/** Round 5's copy names AWS's pooler "the Proxy" without saying AWS. */
const AWS_ROUND_FIVE_WORDS = /\b(AWS|Aurora|RDS|Glue|DMS|Proxy)\b/

function pinned(name: string): string {
  const text = readFileSync(join(import.meta.dirname, 'ringside-cues', name), 'utf8')
  return createHash('sha256').update(text).digest('hex')
}

describe('the Azure edition’s Ringside outcome lines', () => {
  it('are the approved file, pinned as the originals are', () => {
    const text = readFileSync(join(import.meta.dirname, 'ringside-cues', 'outcome-copy-azure.jsonl'), 'utf8')
    expect(createHash('sha256').update(text).digest('hex')).toBe(OUTCOME_COPY_AZURE_SHA256)
    // Rounds 4 and 6's four, and Round 5's six (2026-10-20).
    expect(OUTCOME_COPY_AZURE_RECORDS).toHaveLength(10)
  })

  it('each replace an original that names an AWS product, and name none themselves', () => {
    for (const azure of OUTCOME_COPY_AZURE_RECORDS) {
      const original = OUTCOME_COPY_RECORDS.find(
        (record) => record.round_id === azure.round_id && record.outcome_id === azure.outcome_id,
      )
      expect(original).toBeDefined()
      expect(`${original?.proof_template ?? ''} ${original?.meaning ?? ''}`).toMatch(AWS_WORDS)
      expect(azure.proof_template ?? '').not.toMatch(AWS_WORDS)
      expect(azure.meaning ?? '').not.toMatch(AWS_WORDS)
      expect(azure.copy_mode).toBe(original?.copy_mode)
    }
  })

  it('reach an Azure bout alone', () => {
    const [azure] = OUTCOME_COPY_AZURE_RECORDS
    expect(getOutcomeRecord(azure.round_id, azure.outcome_id, 'azure_postgres')).toBe(azure)
    expect(getOutcomeRecord(azure.round_id, azure.outcome_id, 'rds_postgres')).not.toBe(azure)
    expect(getOutcomeRecord(azure.round_id, azure.outcome_id)).not.toBe(azure)
  })

  it('leave every Azure outcome free of AWS products, Round 5 included', () => {
    for (const record of OUTCOME_COPY_RECORDS) {
      const chosen = getOutcomeRecord(record.round_id, record.outcome_id, 'azure_postgres')
      expect(chosen.proof_template ?? '').not.toMatch(AWS_WORDS)
      expect(chosen.meaning ?? '').not.toMatch(AWS_WORDS)
    }
  })
})

describe('the Azure edition’s Round 5 persona lines', () => {
  // The Round 5 fight card and its ringside cues still said "the selected AWS path adds RDS
  // Proxy" on Azure (2026-10-20): the persona lines had no Azure variants.
  it('are the approved files, pinned as the originals are', () => {
    expect(pinned('verified-corpus-azure.jsonl')).toBe(VERIFIED_CORPUS_AZURE_SHA256)
    expect(pinned('round5-persona-outcomes-azure.jsonl')).toBe(ROUND_FIVE_PERSONA_OUTCOMES_AZURE_SHA256)
    expect(VERIFIED_CORPUS_AZURE_RECORDS).toHaveLength(35)
    expect(ROUND_FIVE_PERSONA_OUTCOME_AZURE_RECORDS).toHaveLength(25)
  })

  it('each replace an original that names AWS, change only what names it, and name none', () => {
    for (const azure of VERIFIED_CORPUS_AZURE_RECORDS) {
      const original = VERIFIED_CORPUS_RECORDS.find((record) => record.round_id === azure.round_id
        && record.persona_id === azure.persona_id && record.priority_key === azure.priority_key)
      expect(original).toBeDefined()
      expect(azure.round_id).toBe('survive_connection_spike')
      expect(`${original?.meaning} ${original?.question}`).toMatch(AWS_ROUND_FIVE_WORDS)
      for (const [text, originalText] of [[azure.meaning, original?.meaning],
        [azure.question, original?.question]] as const) {
        expect(text).not.toMatch(AWS_ROUND_FIVE_WORDS)
        if (!AWS_ROUND_FIVE_WORDS.test(originalText ?? '')) expect(text).toBe(originalText)
      }
    }
    for (const azure of ROUND_FIVE_PERSONA_OUTCOME_AZURE_RECORDS) {
      const original = ROUND_FIVE_PERSONA_OUTCOME_RECORDS.find((record) =>
        record.outcome_id === azure.outcome_id && record.persona_id === azure.persona_id)
      expect(original?.meaning).toMatch(AWS_ROUND_FIVE_WORDS)
      expect(azure.meaning).not.toMatch(AWS_ROUND_FIVE_WORDS)
      // The Lakebase sentence is the original's; only the other path's sentence changes.
      expect(azure.meaning.split('. ')[0]).toBe(original?.meaning.split('. ')[0])
    }
  })

  it('reach an Azure bout alone, and leave every Azure Round 5 line free of AWS', () => {
    const [verified] = VERIFIED_CORPUS_AZURE_RECORDS
    expect(getVerifiedRecord(verified.round_id, verified.persona_id, verified.priority_key,
      'azure_postgres')).toBe(verified)
    expect(getVerifiedRecord(verified.round_id, verified.persona_id, verified.priority_key,
      'rds_postgres')).not.toBe(verified)
    for (const personaId of PERSONA_IDS) {
      for (const priorityKey of PRIORITY_KEYS) {
        const record = getVerifiedRecord('survive_connection_spike', personaId, priorityKey,
          'azure_postgres')
        expect(`${record.meaning} ${record.question}`).not.toMatch(AWS_ROUND_FIVE_WORDS)
        expect(record.meaning.trim().split(/\s+/).length).toBeLessThanOrEqual(34)
        expect(record.question.trim().split(/\s+/).length).toBeLessThanOrEqual(14)
      }
    }
    for (const original of ROUND_FIVE_PERSONA_OUTCOME_RECORDS) {
      const record = getRoundFivePersonaOutcomeRecord(original.outcome_id, original.persona_id,
        'azure_postgres')
      expect(record.meaning).not.toMatch(AWS_ROUND_FIVE_WORDS)
      expect(record.meaning.trim().split(/\s+/).length).toBeLessThanOrEqual(34)
    }
  })

  it('open the Azure fight card in Azure’s words, with the AWS edition’s shape', () => {
    const openings = PERSONA_IDS.map((personaId) =>
      roundFiveFightCardOpening(personaId, 'azure_postgres'))
    expect(new Set(openings).size).toBe(PERSONA_IDS.length)
    for (const opening of openings) {
      expect(opening).not.toMatch(AWS_ROUND_FIVE_WORDS)
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
