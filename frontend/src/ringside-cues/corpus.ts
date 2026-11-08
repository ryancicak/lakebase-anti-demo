import type { CompetitorId, CustomerCorner, PersonaId, RoundId } from '../api/types'
import { competitorKind } from '../competitor-kind'
import lensSource from './lens-lines.jsonl?raw'
import resultSource from './result-lines.jsonl?raw'
import roleSource from './role-lines.jsonl?raw'
import type {
  CompetitorScope,
  LensLineRecord,
  ResultLineRecord,
  RingsideOutcomeId,
  RoleLineRecord,
} from './types'
import { OUTCOME_IDS } from './types'

/**
 * The three files behind "Explain to the room", pinned so a change to what the room
 * is told is a change to this file too, and shows up in review as one.
 */
export const RESULT_LINES_SHA256 = 'e9b0cce68f587446546dd6334cb139453c2b84b493f9761076495bd8d0a4f100'
export const ROLE_LINES_SHA256 = '3bd81e41406a6d2fbc8256a7a4ab32f51e3897b4e7e1188131cb9902215057e0'
export const LENS_LINES_SHA256 = '1346f5d0f7df7098d7a5862fd342523bae19a256b7f3a656068350af675bdc25'

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

export const PRIORITIES = ['cost', 'simplicity', 'performance'] as const satisfies readonly CustomerCorner[]

export const COMPETITOR_SCOPES = ['*', 'aurora', 'rds', 'azure'] as const satisfies readonly CompetitorScope[]

const personaIds = new Set<string>(PERSONA_IDS)
const roundIds = new Set<string>(ROUND_IDS)
const priorities = new Set<string>(PRIORITIES)
const outcomeIds = new Set<string>(OUTCOME_IDS)
const competitorScopes = new Set<string>(COMPETITOR_SCOPES)

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
  if (typeof value !== 'string' || value.trim().length === 0) {
    throw new Error(`${label} has an invalid ${key}.`)
  }
  return value
}

function requireOneOf(
  record: Record<string, unknown>,
  key: string,
  allowed: ReadonlySet<string>,
  label: string,
): string {
  const value = requireString(record, key, label)
  if (!allowed.has(value)) throw new Error(`${label} has unknown ${key} ${value}.`)
  return value
}

function resultLine(value: unknown, index: number): ResultLineRecord {
  const label = `Ringside result line ${index + 1}`
  if (!isObject(value)) throw new Error(`${label} is not an object.`)
  requireOneOf(value, 'round_id', roundIds, label)
  requireOneOf(value, 'outcome_id', outcomeIds, label)
  requireOneOf(value, 'competitor', competitorScopes, label)
  requireString(value, 'text', label)
  return value as unknown as ResultLineRecord
}

function roleLine(value: unknown, index: number): RoleLineRecord {
  const label = `Ringside role line ${index + 1}`
  if (!isObject(value)) throw new Error(`${label} is not an object.`)
  requireOneOf(value, 'round_id', roundIds, label)
  requireOneOf(value, 'persona_id', personaIds, label)
  requireOneOf(value, 'competitor', competitorScopes, label)
  requireString(value, 'why', label)
  requireString(value, 'ask', label)
  return value as unknown as RoleLineRecord
}

function lensLine(value: unknown, index: number): LensLineRecord {
  const label = `Ringside lens line ${index + 1}`
  if (!isObject(value)) throw new Error(`${label} is not an object.`)
  requireOneOf(value, 'round_id', roundIds, label)
  requireOneOf(value, 'priority', priorities, label)
  requireOneOf(value, 'competitor', competitorScopes, label)
  requireString(value, 'text', label)
  return value as unknown as LensLineRecord
}

export const RESULT_LINE_RECORDS = parseJsonl(resultSource, 'Ringside result lines').map(resultLine)
export const ROLE_LINE_RECORDS = parseJsonl(roleSource, 'Ringside role lines').map(roleLine)
export const LENS_LINE_RECORDS = parseJsonl(lensSource, 'Ringside lens lines').map(lensLine)

/**
 * One lookup per file, keyed without the competitor, holding the `*` line and any
 * variants. Every key must have its `*` line, so a bout against a competitor with no
 * variant always has something true to say; a variant without a `*` line is an error.
 */
interface Scoped<T> {
  base: T | null
  variants: Partial<Record<Exclude<CompetitorScope, '*'>, T>>
}

function index<T extends { competitor: CompetitorScope }>(
  records: readonly T[],
  keyOf: (record: T) => string,
  label: string,
): Map<string, Scoped<T>> {
  const byKey = new Map<string, Scoped<T>>()
  for (const record of records) {
    const key = keyOf(record)
    const scoped = byKey.get(key) ?? { base: null, variants: {} }
    if (record.competitor === '*') {
      if (scoped.base) throw new Error(`Duplicate ${label}: ${key}.`)
      scoped.base = record
    } else {
      if (scoped.variants[record.competitor]) {
        throw new Error(`Duplicate ${label} for ${record.competitor}: ${key}.`)
      }
      scoped.variants[record.competitor] = record
    }
    byKey.set(key, scoped)
  }
  for (const [key, scoped] of byKey) {
    if (!scoped.base) throw new Error(`${label} ${key} has a competitor variant but no * line.`)
  }
  return byKey
}

const resultByKey = index(
  RESULT_LINE_RECORDS,
  (record) => `${record.round_id}\0${record.outcome_id}`,
  'Ringside result line',
)
const roleByKey = index(
  ROLE_LINE_RECORDS,
  (record) => `${record.round_id}\0${record.persona_id}`,
  'Ringside role line',
)
const lensByKey = index(
  LENS_LINE_RECORDS,
  (record) => `${record.round_id}\0${record.priority}`,
  'Ringside lens line',
)

function resolve<T>(scoped: Scoped<T> | undefined, competitorId: CompetitorId | undefined, missing: string): T {
  if (!scoped?.base) throw new Error(`Missing ${missing}.`)
  const variant = competitorId ? scoped.variants[competitorKind(competitorId)] : undefined
  return variant ?? scoped.base
}

export function getResultLine(
  roundId: RoundId,
  outcomeId: RingsideOutcomeId,
  competitorId?: CompetitorId,
): ResultLineRecord {
  return resolve(
    resultByKey.get(`${roundId}\0${outcomeId}`),
    competitorId,
    `Ringside result line: ${roundId} × ${outcomeId}`,
  )
}

export function getRoleLine(
  roundId: RoundId,
  personaId: PersonaId,
  competitorId?: CompetitorId,
): RoleLineRecord {
  return resolve(
    roleByKey.get(`${roundId}\0${personaId}`),
    competitorId,
    `Ringside role line: ${roundId} × ${personaId}`,
  )
}

export function getLensLine(
  roundId: RoundId,
  priority: CustomerCorner,
  competitorId?: CompetitorId,
): LensLineRecord {
  return resolve(
    lensByKey.get(`${roundId}\0${priority}`),
    competitorId,
    `Ringside lens line: ${roundId} × ${priority}`,
  )
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

/**
 * The Azure edition's openings, for the personas whose AWS line names AWS's path or a separately
 * provisioned pooler. Azure's pooler is the server's own PgBouncer: no separate service to buy, but
 * a General Purpose or Memory Optimized server, a switch to turn on, a 5,000-client default to
 * raise, and its own port. The other personas' lines say nothing of the other path and hold here.
 */
export const ROUND_FIVE_PERSONA_MEANING_AZURE: Partial<Record<PersonaId, string>> = {
  data_engineer:
    'Lakebase includes pooling for up to 10,000 client connections. Data services get one application path instead of a pooler the team has to switch on and size first.',
  software_engineer:
    'Lakebase includes pooling for up to 10,000 client connections. The selected Azure path switches on a pooler on its own port, which the app team must size and own.',
  architect_it:
    'Lakebase includes pooling for up to 10,000 client connections. The selected Azure path needs a General Purpose or Memory Optimized server, with its pooler switched on and sized before the load arrives.',
  dba:
    'Lakebase includes pooling for up to 10,000 client connections. Client connections are not backend sessions, and on the Azure path you switch on PgBouncer and raise its client limit yourself.',
  executive:
    'Lakebase includes pooling for up to 10,000 client connections. One path already carries that load. The other needs a decision, a configuration step, and a General Purpose server first.',
  infosec:
    'Lakebase includes pooling for up to 10,000 client connections. No new port or setting enters the review, where the selected Azure path opens its pooler on a second port.',
  application_owner:
    'Lakebase includes pooling for up to 10,000 client connections. Learning you need pooling during a launch is expensive, and on the Azure path the pooler must already be on and sized.',
}

/** A persona's Round 5 opening, in the edition's words. */
export function roundFivePersonaMeaning(
  personaId: PersonaId,
  competitorId?: CompetitorId,
): string | undefined {
  const azure = competitorId === 'azure_postgres'
    ? ROUND_FIVE_PERSONA_MEANING_AZURE[personaId]
    : undefined
  return azure ?? ROUND_FIVE_PERSONA_MEANING[personaId]
}
