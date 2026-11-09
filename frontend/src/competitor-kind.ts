import type { CompetitorId } from './api/types'

/**
 * The kind of competitor a session faces, for every choice the screens make between them.
 *
 * "Capabilities, not identities": Phase 1 of the Azure edition, extended for the GCP edition.
 * Each "Aurora, else RDS" choice goes through `byCompetitor`, whose choices are a `Record` over
 * the kinds, and each "RDS, so no scale-to-zero" check goes through `scalesToZero`. A new kind
 * then fails the type check at every such site, which is where its copy and its behavior have to
 * be decided, instead of reading as RDS in silence. The same holds for the competitor's cloud
 * (`competitorCloud`), the fighter labels, the name fragments a display name is read by, and the
 * id list a stored setup is checked against: each is a record over the kinds.
 */
export type CompetitorKind = 'aurora' | 'rds' | 'azure'

/** The cloud a competitor lives in, as share cards, receipts and the room name it: one hashtag-safe word. */
export type CompetitorCloud = 'AWS' | 'Azure'

const KIND_OF_ID: Record<CompetitorId, CompetitorKind> = {
  aurora_serverless_v2: 'aurora',
  rds_postgres: 'rds',
  azure_postgres: 'azure',
}

/** Every competitor id the screens know, exhaustive by construction. */
export const COMPETITOR_IDS = Object.keys(KIND_OF_ID) as readonly CompetitorId[]

export function competitorKind(id: CompetitorId): CompetitorKind {
  return KIND_OF_ID[id]
}

export function byCompetitor<T>(id: CompetitorId, choices: Record<CompetitorKind, T>): T {
  return choices[competitorKind(id)]
}

const CLOUD_OF_KIND: Record<CompetitorKind, CompetitorCloud> = { aurora: 'AWS', rds: 'AWS', azure: 'Azure' }

export function cloudOfKind(kind: CompetitorKind): CompetitorCloud {
  return CLOUD_OF_KIND[kind]
}

/** The competitor's cloud, as share cards and receipts name it. */
export function competitorCloud(id: CompetitorId): CompetitorCloud {
  return cloudOfKind(competitorKind(id))
}

/** The two- or three-letter name the fighter graphics print. */
const FIGHTER_LABEL: Record<CompetitorKind, string> = { aurora: 'AUR', rds: 'RDS', azure: 'AZR' }

export function fighterLabel(id: CompetitorId): string {
  return FIGHTER_LABEL[competitorKind(id)]
}

/**
 * The words in a lane's display name that give its kind away, for the matchup card and the
 * stored history, which carry only names. Checked in this order; a name matching none reads as
 * RDS, as it always has.
 */
const NAME_FRAGMENTS: Record<CompetitorKind, readonly string[]> = {
  azure: ['azure'],
  aurora: ['aurora'],
  rds: ['rds'],
}

export function kindForName(name: string): CompetitorKind {
  const lowered = name.toLowerCase()
  for (const kind of Object.keys(NAME_FRAGMENTS) as CompetitorKind[]) {
    if (NAME_FRAGMENTS[kind].some((fragment) => lowered.includes(fragment))) return kind
  }
  return 'rds'
}

/** The fighter label where only a lane's display name is at hand, as on the matchup card. */
export function fighterLabelForName(name: string): string {
  return FIGHTER_LABEL[kindForName(name)]
}

/**
 * Whether the competitor scales to zero and wakes on a connection, as Round 1 needs. A
 * competitor that can't is checked before the bell and left untimed.
 */
export function scalesToZero(id: CompetitorId): boolean {
  return byCompetitor(id, { aurora: true, rds: false, azure: false })
}
