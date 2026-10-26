// The Azure edition's credits name Azure's own stack, and no AWS product.

import { cleanup, render } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { Credits } from './credits'
import type { CreditsTally } from './credits-tally'
import type { CompetitorDefinition } from './api/types'

vi.mock('./hooks/useReducedMotion', () => ({ useReducedMotion: () => true }))
vi.mock('./audio', () => ({
  playConfirm: vi.fn(),
  playCursor: vi.fn(),
  playOriginalBell: vi.fn(),
  startOriginalCreditsTheme: vi.fn(() => false),
  startOriginalTitleTheme: vi.fn(),
  stopOriginalCreditsTheme: vi.fn(),
  stopOriginalTitleTheme: vi.fn(() => false),
}))

afterEach(cleanup)

function roll(competitor: CompetitorDefinition) {
  const tally: CreditsTally = {
    bouts: 1, lakebaseWins: 1, uncontested: 0, abandoned: 0, competitors: [competitor.short_name],
  }
  return render(<Credits competitors={[competitor]} tally={tally} sound={false} onBack={() => {}} />)
}

describe('the credits roll on each cloud', () => {
  it('names Azure’s runners, control plane and region on an Azure installation', () => {
    const { container } = roll({
      id: 'azure_postgres', name: 'Azure Database for PostgreSQL', short_name: 'Azure PostgreSQL',
      edition: 'AZURE DATABASE FOR POSTGRESQL EDITION',
    } as CompetitorDefinition)
    const text = container.textContent ?? ''
    expect(text).toContain('Run Command-managed Standard_D8ls_v7, one per lane')
    expect(text).toContain('Terraform · Azure Resource Manager · Databricks SDK')
    expect(text).toContain('westus2')
    expect(text).not.toMatch(/\b(AWS|SSM|c7i\.2xlarge|us-west-2)\b/)
  })

  it('keeps AWS’s names on an AWS installation', () => {
    const { container } = roll({
      id: 'aurora_serverless_v2', name: 'Amazon Aurora Serverless v2', short_name: 'Aurora',
      edition: 'PostgreSQL 17',
    } as CompetitorDefinition)
    const text = container.textContent ?? ''
    expect(text).toContain('SSM-managed c7i.2xlarge, one per lane')
    expect(text).toContain('us-west-2')
  })
})
