import type { PersonaId, RoundId } from '../api/types'
import outcomeSource from './outcome-copy.jsonl?raw'
import roundFivePersonaOutcomeSource from './round5-persona-outcomes.jsonl?raw'
import type {
  OutcomeCopyRecord,
  PriorityKey,
  RoundFivePersonaOutcomeRecord,
  RingsideOutcomeId,
  VerifiedCorpusRecord,
} from './types'
import {
  OUTCOME_IDS,
  PRIORITY_KEYS,
} from './types'
import verifiedSource from './verified-corpus.jsonl?raw'

export const VERIFIED_CORPUS_SHA256 = 'e53d5ca576277e0f3e54f3d31212cf08955a121f5cfeb3cd79490db57c94f29f'
export const OUTCOME_COPY_SHA256 = '6a9290423c6b24f0382a95d791d9a1384ecce28d804fcc13913051dafbf4c5fb'
export const ROUND_FIVE_PERSONA_OUTCOMES_SHA256 = '6c651d047288d8fb48bacc043964e3393a9ab267cae6a56dc98870fd43c9281c'

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

function roundFivePersonaOutcomeRecord(
  value: unknown,
  index: number,
): RoundFivePersonaOutcomeRecord {
  const label = `Round 5 persona outcome record ${index + 1}`
  if (!isObject(value)) throw new Error(`${label} is not an object.`)
  const outcomeId = requireString(value, 'outcome_id', label)
  const personaId = requireString(value, 'persona_id', label)
  const allowedOutcomeIds: ReadonlySet<string> = new Set([
    'one_sided_setup_verified_towel',
    'setup_incomplete',
    'bounded_check_failed',
    'cleanup_failed',
    'no_result',
  ])
  if (!allowedOutcomeIds.has(outcomeId)) {
    throw new Error(`${label} has unsupported outcome_id ${outcomeId}.`)
  }
  if (!personaIds.has(personaId)) {
    throw new Error(`${label} has unknown persona_id ${personaId}.`)
  }
  requireString(value, 'meaning_record_id', label)
  requireString(value, 'meaning', label)
  if (value.meaning_decision !== 'KEEP' && value.meaning_decision !== 'REWRITE') {
    throw new Error(`${label} has an invalid meaning_decision.`)
  }
  return value as unknown as RoundFivePersonaOutcomeRecord
}

export const VERIFIED_CORPUS_RECORDS = parseJsonl(verifiedSource, 'Verified Ringside corpus')
  .map(verifiedRecord)
export const OUTCOME_COPY_RECORDS = parseJsonl(outcomeSource, 'Ringside outcome corpus')
  .map(outcomeRecord)
export const ROUND_FIVE_PERSONA_OUTCOME_RECORDS = parseJsonl(
  roundFivePersonaOutcomeSource,
  'Round 5 persona outcome corpus',
).map(roundFivePersonaOutcomeRecord)

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

const roundFivePersonaOutcomeByKey = new Map<string, RoundFivePersonaOutcomeRecord>()
for (const record of ROUND_FIVE_PERSONA_OUTCOME_RECORDS) {
  const key = `${record.outcome_id}\0${record.persona_id}`
  if (roundFivePersonaOutcomeByKey.has(key)) {
    throw new Error(`Duplicate Round 5 persona outcome record: ${key}.`)
  }
  roundFivePersonaOutcomeByKey.set(key, record)
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

export function getRoundFivePersonaOutcomeRecord(
  outcomeId: RingsideOutcomeId,
  personaId: PersonaId,
): RoundFivePersonaOutcomeRecord {
  const record = roundFivePersonaOutcomeByKey.get(`${outcomeId}\0${personaId}`)
  if (!record) {
    throw new Error(`Missing Round 5 persona outcome record: ${outcomeId} × ${personaId}.`)
  }
  return record
}

/**
 * What Round 5 means to each persona, in one line, without proof mechanics.
 *
 * The round is the same for everyone; what it *costs* is not. On the AWS path,
 * pooling for this many clients is a separate managed service to choose, provision,
 * secure and pay for, usually decided before anyone knows it is needed. Lakebase
 * includes it. Every line below is that one asymmetry, told to the person who
 * carries it.
 *
 * `round5.test.tsx` governs the shape: each line names "up to 10,000 client
 * connections", stays inside 34 words, and stays out of the proof vocabulary, so a
 * fight card opens on a human implication rather than a protocol.
 */
export const ROUND_FIVE_PERSONA_MEANING: Record<PersonaId, string> = {
  data_engineer:
    'Lakebase includes pooling for up to 10,000 client connections. Data services get one application path instead of a separate pooler the team has to provision first.',
  software_engineer:
    'Lakebase includes pooling for up to 10,000 client connections. The selected AWS path adds a service the app team must secure and own.',
  data_analyst:
    'Lakebase includes pooling for up to 10,000 client connections. Dashboards keep answering when many people open them at once, with no extra component to plan for.',
  architect_it:
    'Lakebase includes pooling for up to 10,000 client connections. The selected AWS path is a separate managed service to choose, provision, secure, and pay for in advance.',
  data_scientist_ml:
    'Lakebase includes pooling for up to 10,000 client connections. Feature and inference services can open many at once without a pooling tier standing in front of them.',
  dba:
    'Lakebase includes pooling for up to 10,000 client connections. Client connections are not backend sessions, and on the AWS path you provision and keep that pooler healthy yourself.',
  sre:
    'Lakebase includes pooling for up to 10,000 client connections. Connection exhaustion under unplanned load is the page nobody gets, because no extra service had to be running.',
  executive:
    'Lakebase includes pooling for up to 10,000 client connections. One path already carries that load. The other needs a decision, a provisioning step, and a bill first.',
  infosec:
    'Lakebase includes pooling for up to 10,000 client connections. No new endpoint, credential, or managed service enters the review, which the selected AWS path would add.',
  application_owner:
    'Lakebase includes pooling for up to 10,000 client connections. Learning you need pooling during a launch is expensive, and on the AWS path that work must already be done.',
}
