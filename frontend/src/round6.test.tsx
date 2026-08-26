import { cleanup, render, screen, within } from '@testing-library/react'
import { afterEach, describe, expect, it } from 'vitest'
import { RoundSixProof } from './round6'

afterEach(cleanup)

describe('RoundSixProof', () => {
  it('renders one exact three-beat story and a compact verified receipt', () => {
    const { container } = render(
      <RoundSixProof
        state="verified"
        elapsedMs={1_234}
        separateCheckoutVerified
        competitorLabel="Aurora/RDS"
      />,
    )

    expect(screen.getByRole('heading', { level: 1 })).toHaveTextContent(
      'Move live application data into the lakehouse',
    )
    const flow = screen.getByLabelText('Checkout commit to exact Delta answer')
    expect(within(flow).getAllByRole('article')).toHaveLength(3)
    expect(flow).toHaveTextContent('1 × RED-GLOVE · CHICAGO · $84.50 · COMMITTED')
    expect(flow).toHaveTextContent('COMMIT → EXACT DELTA ANSWER')
    expect(flow).toHaveTextContent('1 ORDER · $84.50 REVENUE · EXACT ✓')
    expect(screen.getByRole('timer')).toHaveTextContent('1.23s')
    expect(screen.getByText('SEPARATE CHECKOUT COMMITTED ✓')).toBeInTheDocument()
    expect(screen.getByLabelText('Round 6 receipt')).toHaveTextContent(
      'ORDER INCLUDED ✓ · COUNT VERIFIED ✓ · SEPARATE CHECKOUT COMMITTED ✓',
    )
    expect(screen.getByText(/Aurora\/RDS/).parentElement).toHaveTextContent(
      'requires a separate CDC stack · not built or timed',
    )
    expect(screen.getByText('PUBLIC PREVIEW · SEPARATE DELTA HISTORY · THROUGHPUT + P99 NOT MEASURED')).toBeInTheDocument()
    expect(container.querySelectorAll('.round6-beat')).toHaveLength(3)
  })

  it('shows one running freshness timer without inventing a completed receipt', () => {
    render(
      <RoundSixProof
        state="running"
        elapsedMs={630}
        separateCheckoutVerified={false}
      />,
    )

    expect(screen.getByRole('timer')).toHaveTextContent(/^0\.6\d+s$/)
    expect(screen.getByText('WAITING FOR EXACT ANSWER')).toBeInTheDocument()
    expect(screen.getByText('SEPARATE CHECKOUT NOT VERIFIED')).toBeInTheDocument()
    expect(screen.queryByLabelText('Round 6 receipt')).not.toBeInTheDocument()
  })

  it('keeps raw proof internals and unsupported performance claims off the screen', () => {
    render(
      <RoundSixProof
        state="failed"
        elapsedMs={null}
        separateCheckoutVerified={false}
        status="The exact analytical answer was not verified."
      />,
    )

    expect(screen.getByRole('alert')).toHaveTextContent('The exact analytical answer was not verified.')
    expect(screen.queryByLabelText('Round 6 receipt')).not.toBeInTheDocument()
    expect(document.body).not.toHaveTextContent(/nonce|lsn|change data feed|throughput result|p99 result/i)
  })
})
