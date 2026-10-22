import type { CompetitorId } from './api/types'

/**
 * The kind of competitor a session faces, for every choice the screens make between them.
 *
 * Phase 1 of the Azure edition ("capabilities, not identities"). Each "Aurora, else RDS" choice
 * goes through `byCompetitor`, whose choices are a `Record` over the kinds, and each "RDS, so no
 * scale-to-zero" check goes through `scalesToZero`. A third kind then fails the type check at
 * every such site, which is where its copy and its behavior have to be decided, instead of
 * reading as RDS in silence.
 */
export type CompetitorKind = 'aurora' | 'rds' | 'azure'

export function competitorKind(id: CompetitorId): CompetitorKind {
  switch (id) {
    case 'aurora_serverless_v2':
      return 'aurora'
    case 'rds_postgres':
      return 'rds'
    case 'azure_postgres':
      return 'azure'
  }
}

export function byCompetitor<T>(id: CompetitorId, choices: Record<CompetitorKind, T>): T {
  return choices[competitorKind(id)]
}

/** The two- or three-letter name the fighter graphics print. */
export function fighterLabel(id: CompetitorId): string {
  return byCompetitor(id, { aurora: 'AUR', rds: 'RDS', azure: 'AZR' })
}

/** The fighter label where only a lane's display name is at hand, as on the matchup card. */
export function fighterLabelForName(name: string): string {
  const lowered = name.toLowerCase()
  if (lowered.includes('azure')) return 'AZR'
  return lowered.includes('aurora') ? 'AUR' : 'RDS'
}

/**
 * Whether the competitor scales to zero and wakes on a connection, as Round 1 needs. A
 * competitor that can't is checked before the bell and left untimed.
 */
export function scalesToZero(id: CompetitorId): boolean {
  return byCompetitor(id, { aurora: true, rds: false, azure: false })
}
