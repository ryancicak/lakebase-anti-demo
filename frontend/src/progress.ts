import type {
  CompetitorId,
  CustomerCorner,
  PersonaId,
  RoundId,
} from './api/types'

/**
 * `summary` is session-less on purpose. It reads sealed receipts, so it is the one
 * stage that still has something to show after the server has been restarted and
 * the in-memory session store has come back empty.
 */
export type Stage = 'title' | 'setup' | 'matchup' | 'ready' | 'proof' | 'between' | 'scorecard' | 'finale' | 'summary'
export type SetupScene = 'opponent' | 'lead' | 'lenses' | 'card'
export type HistoryMode = 'push' | 'replace'

export interface BrowserView {
  stage: Stage
  setupScene: SetupScene
}

export interface SetupProgress {
  stage: 'title' | 'setup'
  setupScene: SetupScene
  competitor: CompetitorId
  corners: CustomerCorner[]
  primary: PersonaId
  secondary: PersonaId[]
  roundOverride: RoundId | null
  sound: boolean
}

const STORAGE_KEY = 'lakebase-anti-demo:setup:v1'
const HISTORY_KEY = 'lakebaseAntiDemoView'

const competitors = new Set<CompetitorId>(['aurora_serverless_v2', 'rds_postgres'])
const corners = new Set<CustomerCorner>(['cost', 'simplicity', 'performance'])
const personas = new Set<PersonaId>([
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
])
const rounds = new Set<RoundId>([
  'wake_idle_app',
  'make_schema_change_safely',
  'recover_deleted_order',
  'put_model_score_in_app',
  'survive_connection_spike',
  'analyze_live_orders_without_slowing_checkout',
])
const stages = new Set<Stage>(['title', 'setup', 'matchup', 'ready', 'proof', 'between', 'scorecard', 'finale', 'summary'])
const setupScenes = new Set<SetupScene>(['opponent', 'lead', 'lenses', 'card'])

const defaults: SetupProgress = {
  stage: 'title',
  setupScene: 'opponent',
  competitor: 'aurora_serverless_v2',
  corners: ['performance'],
  primary: 'sre',
  secondary: [],
  roundOverride: null,
  sound: true,
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function oneOf<T extends string>(value: unknown, allowed: Set<T>, fallback: T): T {
  return typeof value === 'string' && allowed.has(value as T) ? value as T : fallback
}

export function loadSetupProgress(): SetupProgress {
  try {
    const encoded = window.localStorage.getItem(STORAGE_KEY)
    if (!encoded) return defaults
    const value: unknown = JSON.parse(encoded)
    if (!isRecord(value)) return defaults

    const primary = oneOf(value.primary, personas, defaults.primary)
    const selectedCorners = Array.isArray(value.corners)
      ? value.corners.filter((item): item is CustomerCorner => (
          typeof item === 'string' && corners.has(item as CustomerCorner)
        ))
      : []
    const selectedSecondaries = Array.isArray(value.secondary)
      ? [...new Set(value.secondary.filter((item): item is PersonaId => (
          typeof item === 'string'
          && personas.has(item as PersonaId)
          && item !== primary
        )))].slice(0, 2)
      : []
    const roundOverride = typeof value.roundOverride === 'string'
      && rounds.has(value.roundOverride as RoundId)
      ? value.roundOverride as RoundId
      : null

    const progress: SetupProgress = {
      stage: value.stage === 'setup' ? 'setup' : 'title',
      setupScene: oneOf(value.setupScene, setupScenes, defaults.setupScene),
      competitor: oneOf(value.competitor, competitors, defaults.competitor),
      corners: selectedCorners.length ? [...new Set(selectedCorners)] : defaults.corners,
      primary,
      secondary: selectedSecondaries,
      roundOverride,
      sound: typeof value.sound === 'boolean' ? value.sound : defaults.sound,
    }
    return window.location.hash === '#title'
      ? { ...progress, stage: 'title', setupScene: 'opponent' }
      : progress
  } catch {
    return defaults
  }
}

export function saveSetupProgress(progress: Omit<SetupProgress, 'stage'> & { stage: Stage }): void {
  const safe: SetupProgress = {
    ...progress,
    stage: progress.stage === 'title' ? 'title' : 'setup',
    setupScene: progress.stage === 'setup' ? progress.setupScene : 'card',
  }
  try {
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(safe))
  } catch {
    // Browser storage can be unavailable in private or locked-down contexts.
  }
}

export function readBrowserView(state: unknown): BrowserView | null {
  if (!isRecord(state)) return null
  const candidate = state[HISTORY_KEY]
  if (!isRecord(candidate)) return null
  if (
    typeof candidate.stage !== 'string'
    || !stages.has(candidate.stage as Stage)
    || typeof candidate.setupScene !== 'string'
    || !setupScenes.has(candidate.setupScene as SetupScene)
  ) return null
  return {
    stage: candidate.stage as Stage,
    setupScene: candidate.setupScene as SetupScene,
  }
}

export function writeBrowserView(view: BrowserView, mode: HistoryMode): void {
  const current = isRecord(window.history.state) ? window.history.state : {}
  const state = { ...current, [HISTORY_KEY]: view }
  const hash = view.stage === 'setup' ? `#setup/${view.setupScene}` : `#${view.stage}`
  const url = `${window.location.pathname}${window.location.search}${hash}`
  if (mode === 'push') window.history.pushState(state, '', url)
  else window.history.replaceState(state, '', url)
}

export function requiresSession(stage: Stage): boolean {
  return stage === 'matchup' || stage === 'ready' || stage === 'proof' || stage === 'between' || stage === 'finale'
}
