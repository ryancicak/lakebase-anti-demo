import { describe, expect, it } from 'vitest'

import type { CompetitorId } from './api/types'
import { byCompetitor, competitorKind, fighterLabel, scalesToZero } from './competitor-kind'
import { opponentBadge } from './ring'
import { roundFourStackLabel } from './round4'
import { ROUND_SIX_CONDITION, roundSixCondition, roundSixStackLabel } from './round6'

describe('competitor kind', () => {
  it('maps the two AWS competitors as the screens always have', () => {
    expect(competitorKind('aurora_serverless_v2')).toBe('aurora')
    expect(competitorKind('rds_postgres')).toBe('rds')
    expect(fighterLabel('aurora_serverless_v2')).toBe('AUR')
    expect(fighterLabel('rds_postgres')).toBe('RDS')
  })

  it('maps the Azure edition competitor as its own kind', () => {
    expect(competitorKind('azure_postgres')).toBe('azure')
    expect(fighterLabel('azure_postgres')).toBe('AZR')
  })

  it('gives the ring badge the fighter label for every competitor', () => {
    // The ring's badge, the share cards and the finale print this. On the Azure edition's
    // first deploy it read ??? (2026-10-19).
    for (const id of ['aurora_serverless_v2', 'rds_postgres', 'azure_postgres'] as const) {
      expect(opponentBadge(id)).toBe(fighterLabel(id))
    }
    expect(opponentBadge('a_future_competitor' as CompetitorId)).toBe('???')
  })

  it('names each edition’s own integration in Rounds 4 and 6', () => {
    // The Azure edition's first browser bouts read "AWS Glue → Azure PostgreSQL" and
    // "AWS: DMS + Glue (cold start)" (2026-10-19).
    const azure = { competitor: { id: 'azure_postgres' as const, name: 'Azure Database for PostgreSQL', short_name: 'Azure PostgreSQL', edition: 'AZURE DATABASE FOR POSTGRESQL EDITION' } }
    const rds = { competitor: { id: 'rds_postgres' as const, name: 'Amazon RDS for PostgreSQL', short_name: 'RDS PostgreSQL', edition: 'RDS POSTGRESQL EDITION' } }
    expect(roundFourStackLabel(azure, 'competitor')).toBe('Azure Data Factory → Azure PostgreSQL')
    expect(roundSixStackLabel(azure, 'competitor')).toBe('Azure Data Factory from Azure PostgreSQL')
    expect(roundSixCondition('azure_postgres')).toBe('Lakebase: built-in change feed · Azure: Data Factory (cold start)')
    // The AWS edition's words are what they were.
    expect(roundFourStackLabel(rds, 'competitor')).toBe('AWS Glue → RDS PostgreSQL')
    expect(roundSixStackLabel(rds, 'competitor')).toBe('AWS DMS + Glue from RDS PostgreSQL')
    expect(roundSixCondition('aurora_serverless_v2')).toBe(ROUND_SIX_CONDITION)
    expect(roundFourStackLabel(azure, 'lakebase')).toBe('Lakebase synced table')
  })

  it('knows Round 1 can only time a competitor that scales to zero', () => {
    expect(scalesToZero('aurora_serverless_v2')).toBe(true)
    expect(scalesToZero('rds_postgres')).toBe(false)
    expect(scalesToZero('azure_postgres')).toBe(false)
  })

  it('picks the choice for the session competitor kind', () => {
    const choices = { aurora: 'clone', rds: 'restore', azure: 'restore to a new server' }
    expect(byCompetitor('aurora_serverless_v2', choices)).toBe('clone')
    expect(byCompetitor('rds_postgres', choices)).toBe('restore')
    expect(byCompetitor('azure_postgres', choices)).toBe('restore to a new server')
  })
})
