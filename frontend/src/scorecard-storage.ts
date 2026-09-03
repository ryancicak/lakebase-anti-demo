import type { CooldownLaneState, CooldownSnapshot, RoundId } from './api/types'
import type { ContractStatus, EvidenceShape, FormalWinner } from './outcome'

export interface ScorecardEntry {
  session_id: string
  round_id?: RoundId
  round_title: string
  competitor: string
  lakebase_ms: number | null
  competitor_ms: number | null
  competitor_censored?: boolean
  competitor_capability_gap?: boolean
  evidence_shape?: EvidenceShape
  contract_status?: ContractStatus
  formal_winner?: FormalWinner
  margin_ms?: number | null
  remembered_result: string
  completed_at: string
  cooldown: {
    mode: CooldownSnapshot['mode']
    lakebase_ms: number | null
    competitor_ms: number | null
    lakebase_state?: CooldownLaneState
    competitor_state?: CooldownLaneState
  } | null
}

export const SCORECARD_STORAGE_KEY = 'lakebase-anti-demo:scorecard:v2'
export const LEGACY_SCORECARD_STORAGE_KEY = 'lakebase-anti-demo:scorecard:v1'
const SCORECARD_SCHEMA_VERSION = 2

const ROUND_IDS = new Set<RoundId>([
  'wake_idle_app',
  'make_schema_change_safely',
  'recover_deleted_order',
  'put_model_score_in_app',
  'survive_connection_spike',
  'analyze_live_orders_without_slowing_checkout',
])
const EVIDENCE_SHAPES = new Set<EvidenceShape>([
  'both_exact_verified', 'lakebase_only_exact', 'competitor_only_exact',
  'exact_and_censored_lower_bound', 'both_lower_bounds', 'neither_verified',
  'capability_gap', 'guardrail_failure', 'cleanup_failure',
])
const CONTRACT_STATUSES = new Set<ContractStatus>([
  'declared_comparison', 'declared_capability', 'adjudicated_stoppage',
  'comparison_incomplete', 'guardrail_failure', 'cleanup_failure',
  'no_verified_evidence',
])
const WINNERS = new Set<Exclude<FormalWinner, null>>(['lakebase', 'competitor', 'tie'])
const COOLDOWN_MODES = new Set<CooldownSnapshot['mode']>([
  'return_to_idle', 'delete_isolated_environment', 'delete_recovery_environment',
])
const COOLDOWN_STATES = new Set<CooldownLaneState>([
  'watching', 'confirmed_zero', 'confirmed_deleted', 'not_supported', 'failed',
])

function record(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null
}

function finiteTime(value: unknown): value is number | null {
  return value === null || (typeof value === 'number' && Number.isFinite(value) && value >= 0)
}

function optionalBoolean(value: unknown): boolean {
  return value === undefined || typeof value === 'boolean'
}

function optionalMember<T extends string>(value: unknown, values: Set<T>): value is T | undefined {
  return value === undefined || (typeof value === 'string' && values.has(value as T))
}

function validCooldown(value: unknown): value is ScorecardEntry['cooldown'] {
  if (value === null) return true
  const item = record(value)
  return Boolean(
    item
    && optionalMember(item.mode, COOLDOWN_MODES)
    && item.mode !== undefined
    && finiteTime(item.lakebase_ms)
    && finiteTime(item.competitor_ms)
    && optionalMember(item.lakebase_state, COOLDOWN_STATES)
    && optionalMember(item.competitor_state, COOLDOWN_STATES),
  )
}

export function validScorecardEntry(value: unknown): value is ScorecardEntry {
  const item = record(value)
  if (!item) return false
  return (
    typeof item.session_id === 'string' && item.session_id.trim().length > 0
    && optionalMember(item.round_id, ROUND_IDS)
    && typeof item.round_title === 'string' && item.round_title.trim().length > 0
    && typeof item.competitor === 'string' && item.competitor.trim().length > 0
    && finiteTime(item.lakebase_ms)
    && finiteTime(item.competitor_ms)
    && optionalBoolean(item.competitor_censored)
    && optionalBoolean(item.competitor_capability_gap)
    && optionalMember(item.evidence_shape, EVIDENCE_SHAPES)
    && optionalMember(item.contract_status, CONTRACT_STATUSES)
    && (
      item.formal_winner === undefined
      || item.formal_winner === null
      || (typeof item.formal_winner === 'string' && WINNERS.has(item.formal_winner as Exclude<FormalWinner, null>))
    )
    && (item.margin_ms === undefined || finiteTime(item.margin_ms))
    && typeof item.remembered_result === 'string' && item.remembered_result.trim().length > 0
    && typeof item.completed_at === 'string' && Number.isFinite(Date.parse(item.completed_at))
    && validCooldown(item.cooldown)
  )
}

function parseEntries(value: unknown): ScorecardEntry[] | null {
  if (!Array.isArray(value) || !value.every(validScorecardEntry)) return null
  return value
}

export function parseScorecardStorage(value: unknown): ScorecardEntry[] {
  const envelope = record(value)
  if (!envelope || envelope.version !== SCORECARD_SCHEMA_VERSION) return []
  return parseEntries(envelope.entries) ?? []
}

export function loadScorecard(): ScorecardEntry[] {
  try {
    const current = window.localStorage.getItem(SCORECARD_STORAGE_KEY)
    if (current !== null) return parseScorecardStorage(JSON.parse(current))
    const legacy = window.localStorage.getItem(LEGACY_SCORECARD_STORAGE_KEY)
    if (legacy === null) return []
    const migrated = parseEntries(JSON.parse(legacy))
    if (!migrated) return []
    saveScorecard(migrated)
    window.localStorage.removeItem(LEGACY_SCORECARD_STORAGE_KEY)
    return migrated
  } catch {
    return []
  }
}

export function saveScorecard(entries: ScorecardEntry[]): void {
  window.localStorage.setItem(SCORECARD_STORAGE_KEY, JSON.stringify({
    version: SCORECARD_SCHEMA_VERSION,
    entries,
  }))
}
