import type { PersonaId, RoundId } from '../api/types'
import outcomeSource from './outcome-copy.jsonl?raw'
import type {
  OutcomeCopyRecord,
  PriorityKey,
  RingsideOutcomeId,
  VerifiedCorpusRecord,
} from './types'
import {
  OUTCOME_IDS,
  PRIORITY_KEYS,
} from './types'
import verifiedSource from './verified-corpus.jsonl?raw'

export const VERIFIED_CORPUS_SHA256 = '55bd71d5058e8388f358f8d0d34ab65f0326a6e0607ee8b5b774d53fa3820769'
export const OUTCOME_COPY_SHA256 = 'b2e12cb141630d72bd2718c50c2266d28829a9be4752c3b14b65c0a8428bf276'

export const PERSONA_IDS = [
  'data_engineer',
  'software_engineer',
  'data_analyst',
  'architect_it',
  'data_scientist_ml',
  'dba',
  'sre',
  'executive',
  'infosec',
  'application_owner',
] as const satisfies readonly PersonaId[]

export const ROUND_IDS = [
  'wake_idle_app',
  'make_schema_change_safely',
  'recover_deleted_order',
  'put_model_score_in_app',
  'survive_connection_spike',
  'analyze_live_orders_without_slowing_checkout',
] as const satisfies readonly RoundId[]

const personaIds = new Set<string>(PERSONA_IDS)
const roundIds = new Set<string>(ROUND_IDS)
const priorityKeys = new Set<string>(PRIORITY_KEYS)
const outcomeIds = new Set<string>(OUTCOME_IDS)

function parseJsonl(source: string, label: string): unknown[] {
  const lines = source.split(/\r?\n/).filter((line) => line.trim().length > 0)
  if (lines.length === 0) throw new Error(`${label} is empty.`)
  return lines.map((line, index) => {
    try {
      return JSON.parse(line) as unknown
    } catch {
      throw new Error(`${label} line ${index + 1} is not valid JSON.`)
    }
  })
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function requireString(record: Record<string, unknown>, key: string, label: string): string {
  const value = record[key]
  if (typeof value !== 'string' || value.length === 0) {
    throw new Error(`${label} has an invalid ${key}.`)
  }
  return value
}

function verifiedRecord(value: unknown, index: number): VerifiedCorpusRecord {
  const label = `Verified Ringside record ${index + 1}`
  if (!isObject(value)) throw new Error(`${label} is not an object.`)
  const roundId = requireString(value, 'round_id', label)
  const personaId = requireString(value, 'persona_id', label)
  const priorityKey = requireString(value, 'priority_key', label)
  const meaningDecision = requireString(value, 'meaning_decision', label)
  const questionDecision = requireString(value, 'question_decision', label)
  if (!roundIds.has(roundId)) throw new Error(`${label} has unknown round_id ${roundId}.`)
  if (!personaIds.has(personaId)) throw new Error(`${label} has unknown persona_id ${personaId}.`)
  if (!priorityKeys.has(priorityKey)) throw new Error(`${label} has unknown priority_key ${priorityKey}.`)
  if (meaningDecision !== 'KEEP' && meaningDecision !== 'REWRITE') {
    throw new Error(`${label} has invalid meaning_decision ${meaningDecision}.`)
  }
  if (questionDecision !== 'KEEP' && questionDecision !== 'REWRITE') {
    throw new Error(`${label} has invalid question_decision ${questionDecision}.`)
  }
  requireString(value, 'meaning_record_id', label)
  requireString(value, 'question_record_id', label)
  requireString(value, 'meaning', label)
  requireString(value, 'question', label)
  return value as unknown as VerifiedCorpusRecord
}

function outcomeRecord(value: unknown, index: number): OutcomeCopyRecord {
  const label = `Ringside outcome record ${index + 1}`
  if (!isObject(value)) throw new Error(`${label} is not an object.`)
  const roundId = requireString(value, 'round_id', label)
  const outcomeId = requireString(value, 'outcome_id', label)
  const copyMode = requireString(value, 'copy_mode', label)
  if (!roundIds.has(roundId)) throw new Error(`${label} has unknown round_id ${roundId}.`)
  if (!outcomeIds.has(outcomeId)) throw new Error(`${label} has unknown outcome_id ${outcomeId}.`)
  if (value.persona_id !== '*' || value.priority_key !== '*') {
    throw new Error(`${label} must apply to every persona and priority.`)
  }
  if (copyMode !== 'INHERIT_VERIFIED_CORPUS' && copyMode !== 'OUTCOME_OVERRIDE') {
    throw new Error(`${label} has invalid copy_mode ${copyMode}.`)
  }
  requireString(value, 'proof_template_id', label)
  requireString(value, 'proof_template', label)
  if (value.proof_decision !== 'KEEP' && value.proof_decision !== 'REWRITE') {
    throw new Error(`${label} has an invalid proof_decision.`)
  }
  if (copyMode === 'OUTCOME_OVERRIDE') {
    requireString(value, 'meaning_record_id', label)
    requireString(value, 'question_record_id', label)
    requireString(value, 'meaning', label)
    requireString(value, 'question', label)
  }
  return value as unknown as OutcomeCopyRecord
}

export const VERIFIED_CORPUS_RECORDS = parseJsonl(verifiedSource, 'Verified Ringside corpus')
  .map(verifiedRecord)
export const OUTCOME_COPY_RECORDS = parseJsonl(outcomeSource, 'Ringside outcome corpus')
  .map(outcomeRecord)

const verifiedByKey = new Map<string, VerifiedCorpusRecord>()
for (const record of VERIFIED_CORPUS_RECORDS) {
  const key = `${record.round_id}\0${record.persona_id}\0${record.priority_key}`
  if (verifiedByKey.has(key)) throw new Error(`Duplicate verified Ringside record: ${key}.`)
  verifiedByKey.set(key, record)
}

const outcomeByKey = new Map<string, OutcomeCopyRecord>()
for (const record of OUTCOME_COPY_RECORDS) {
  const key = `${record.round_id}\0${record.outcome_id}`
  if (outcomeByKey.has(key)) throw new Error(`Duplicate Ringside outcome record: ${key}.`)
  outcomeByKey.set(key, record)
}

export function getVerifiedRecord(
  roundId: RoundId,
  personaId: PersonaId,
  priorityKey: PriorityKey,
): VerifiedCorpusRecord {
  const record = verifiedByKey.get(`${roundId}\0${personaId}\0${priorityKey}`)
  if (!record) {
    throw new Error(`Missing verified Ringside record: ${roundId} × ${personaId} × ${priorityKey}.`)
  }
  return record
}

export function getOutcomeRecord(
  roundId: RoundId,
  outcomeId: RingsideOutcomeId,
): OutcomeCopyRecord {
  const record = outcomeByKey.get(`${roundId}\0${outcomeId}`)
  if (!record) throw new Error(`Missing Ringside outcome record: ${roundId} × ${outcomeId}.`)
  return record
}
