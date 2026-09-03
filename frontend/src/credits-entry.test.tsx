import { cleanup, render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { CreditsButton, type CreditsEntry } from './credits-entry'
import type { CompetitorDefinition } from './api/types'

vi.mock('./hooks/useReducedMotion', () => ({ useReducedMotion: () => true }))

/* The roll asks audio.ts for its own cue and takes no for an answer; it never
   reaches past that for a round theme. Every theme function is mocked so the
   tests can assert exactly which ones it touches. */
const audioMocks = vi.hoisted(() => ({
  playConfirm: vi.fn(),
  playCursor: vi.fn(),
  playOriginalBell: vi.fn(),
  playStart: vi.fn(),
  setOriginalCreditsThemeMuted: vi.fn(),
  setOriginalRoundThemeMuted: vi.fn(),
  setOriginalTitleThemeMuted: vi.fn(),
  startOriginalCreditsTheme: vi.fn(),
  startOriginalRoundTheme: vi.fn(),
  startOriginalTitleTheme: vi.fn(),
  stopOriginalCreditsTheme: vi.fn(),
  stopOriginalRoundTheme: vi.fn(),
  stopOriginalTitleTheme: vi.fn(),
}))
vi.mock('./audio', () => audioMocks)

const competitors: CompetitorDefinition[] = [
  {
    id: 'aurora_serverless_v2',
    name: 'Amazon Aurora Serverless v2',
    short_name: 'Aurora',
    edition: 'PostgreSQL 17',
  } as CompetitorDefinition,
]

function entry(overrides: Partial<CreditsEntry> = {}): CreditsEntry {
  return {
    competitors,
    scorecard: [{ competitor: 'Aurora', lakebase_ms: 900, competitor_ms: 4200 }],
    sound: false,
    boutInFlight: false,
    ...overrides,
  }
}

/** A stand-in for the screen the trigger lives on, so the tests can assert the
 *  screen underneath is still mounted while the roll is up. */
function Host({ value }: { value: CreditsEntry }) {
  return (
    <main>
      <p data-testid="arena">Round in progress</p>
      <CreditsButton entry={value} className="credits-entry">Credits</CreditsButton>
    </main>
  )
}

function trigger() {
  return screen.getByRole('button', { name: /credits/i })
}

beforeEach(() => {
  Object.values(audioMocks).forEach((mock) => mock.mockClear())
})

afterEach(cleanup)

describe('the credits entry point', () => {
  it('renders a labelled control and keeps the roll closed until it is used', () => {
    render(<Host value={entry()} />)
    expect(trigger()).toHaveAttribute('aria-expanded', 'false')
    expect(screen.queryByRole('dialog', { name: 'Credits' })).not.toBeInTheDocument()
  })

  it('opens the roll without unmounting the screen underneath', async () => {
    const user = userEvent.setup()
    render(<Host value={entry()} />)

    await user.click(trigger())

    const dialog = screen.getByRole('dialog', { name: 'Credits' })
    expect(dialog).toBeInTheDocument()
    // Portalled to <body>, so it is rendered in addition to the host, not instead of it.
    expect(dialog.parentElement).toBe(document.body)
    expect(screen.getByTestId('arena')).toBeInTheDocument()
    expect(trigger()).toHaveAttribute('aria-expanded', 'true')
  })

  it('leads with the contribute hook, then the contributors', async () => {
    const user = userEvent.setup()
    render(<Host value={entry()} />)

    await user.click(trigger())

    const dialog = screen.getByRole('dialog', { name: 'Credits' })
    // Both are billed, and in this order: the invitation is the only beat on
    // the roll about somebody who has not committed yet, so it takes the top
    // slot; the crew follows immediately so the two read as one unit.
    expect(within(dialog).getByText('Now your turn')).toBeInTheDocument()
    expect(within(dialog).getByText('Built by')).toBeInTheDocument()
    expect(within(dialog).getByText('Ryan Cicak')).toBeInTheDocument()

    const order = Array.from(dialog.querySelector('.credits-roll')!.children)
    expect(order[1].className).toContain('credits-invite')
    expect(order[2].className).toContain('credits-crew')
  })

  it('opens the roll on the hook rather than on the author credit', async () => {
    const user = userEvent.setup()
    render(<Host value={entry()} />)

    await user.click(trigger())

    const dialog = screen.getByRole('dialog', { name: 'Credits' })
    const hook = dialog.querySelector('.credits-roll .credits-invite') as HTMLElement
    // Scoped to the crawl: the old layout also named the author on the held
    // card, which follows the hook wherever the hook sits.
    const credit = dialog.querySelector('.credits-roll .production-credit') as HTMLElement
    expect(hook).not.toBeNull()
    expect(credit).not.toBeNull()
    expect(hook.compareDocumentPosition(credit) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()
    // And the hook is the first beat after the title card, not merely earlier.
    expect(hook.previousElementSibling?.className).toContain('credits-title')
  })

  it('moves focus to the exit on open', async () => {
    const user = userEvent.setup()
    render(<Host value={entry()} />)

    await user.click(trigger())

    await waitFor(() => {
      expect(screen.getByRole('button', { name: 'B · Back' })).toHaveFocus()
    })
  })

  it('closes on B · Back and returns focus to the trigger', async () => {
    const user = userEvent.setup()
    render(<Host value={entry()} />)

    await user.click(trigger())
    await user.click(screen.getByRole('button', { name: 'B · Back' }))

    expect(screen.queryByRole('dialog', { name: 'Credits' })).not.toBeInTheDocument()
    await waitFor(() => expect(trigger()).toHaveFocus())
  })

  it('closes on Escape and returns focus to the trigger', async () => {
    const user = userEvent.setup()
    render(<Host value={entry()} />)

    await user.click(trigger())
    await user.keyboard('{Escape}')

    expect(screen.queryByRole('dialog', { name: 'Credits' })).not.toBeInTheDocument()
    await waitFor(() => expect(trigger()).toHaveFocus())
  })

  it('does not leave an Escape handler behind once it is closed', async () => {
    const user = userEvent.setup()
    const addListener = vi.spyOn(document, 'addEventListener')
    const removeListener = vi.spyOn(document, 'removeEventListener')
    render(<Host value={entry()} />)

    await user.click(trigger())
    await user.keyboard('{Escape}')

    const added = addListener.mock.calls.filter(([type]) => type === 'keydown').length
    const removed = removeListener.mock.calls.filter(([type]) => type === 'keydown').length
    expect(added).toBeGreaterThan(0)
    expect(removed).toBe(added)
    expect(addListener).toHaveBeenCalledWith('keydown', expect.any(Function), true)
    addListener.mockRestore()
    removeListener.mockRestore()
  })

  it('asks for its own cue on open and gives the transport back on close', async () => {
    const user = userEvent.setup()
    audioMocks.stopOriginalTitleTheme.mockReturnValue(true)
    audioMocks.startOriginalCreditsTheme.mockReturnValue(true)
    render(<Host value={entry({ sound: true })} />)

    await user.click(trigger())
    expect(audioMocks.startOriginalCreditsTheme).toHaveBeenCalledTimes(1)
    // The attract loop is the only thing the roll displaces, and it is handed
    // straight back. A round cue is never reached for at all.
    expect(audioMocks.stopOriginalTitleTheme).toHaveBeenCalledTimes(1)
    expect(audioMocks.startOriginalTitleTheme).not.toHaveBeenCalled()

    await user.keyboard('{Escape}')
    expect(audioMocks.stopOriginalCreditsTheme).toHaveBeenCalledTimes(1)
    expect(audioMocks.startOriginalTitleTheme).toHaveBeenCalledTimes(1)
    expect(audioMocks.startOriginalRoundTheme).not.toHaveBeenCalled()
    expect(audioMocks.stopOriginalRoundTheme).not.toHaveBeenCalled()
  })

  it('leaves a live bout alone when audio.ts declines it the transport', async () => {
    const user = userEvent.setup()
    // What audio.ts returns while a round cue holds the transport.
    audioMocks.stopOriginalTitleTheme.mockReturnValue(false)
    audioMocks.startOriginalCreditsTheme.mockReturnValue(false)
    render(<Host value={entry({ sound: true, boutInFlight: true })} />)

    await user.click(trigger())
    await user.keyboard('{Escape}')

    // Nothing was started, and nothing was left needing to be given back.
    expect(audioMocks.startOriginalRoundTheme).not.toHaveBeenCalled()
    expect(audioMocks.stopOriginalRoundTheme).not.toHaveBeenCalled()
    expect(audioMocks.startOriginalTitleTheme).not.toHaveBeenCalled()
    expect(audioMocks.stopOriginalCreditsTheme).not.toHaveBeenCalled()
  })

  it('starts no cue at all when sound is off', async () => {
    const user = userEvent.setup()
    render(<Host value={entry({ sound: false })} />)

    await user.click(trigger())

    expect(audioMocks.startOriginalCreditsTheme).not.toHaveBeenCalled()
    expect(audioMocks.stopOriginalTitleTheme).not.toHaveBeenCalled()
  })

  it('stays silent altogether when sound is off', async () => {
    const user = userEvent.setup()
    render(<Host value={entry({ sound: false })} />)

    await user.click(trigger())

    expect(audioMocks.playConfirm).not.toHaveBeenCalled()
    expect(audioMocks.playCursor).not.toHaveBeenCalled()
  })

  it('says the round is still running, and rings no bell, over a live bout', async () => {
    const user = userEvent.setup()
    render(<Host value={entry({ sound: true, boutInFlight: true })} />)

    await user.click(trigger())

    expect(screen.getByText(/Round still running · B or Esc to return/i)).toBeInTheDocument()
    expect(audioMocks.playOriginalBell).not.toHaveBeenCalled()
  })

  it('does not claim a round is running when none is', async () => {
    const user = userEvent.setup()
    render(<Host value={entry({ boutInFlight: false })} />)

    await user.click(trigger())

    expect(screen.queryByText(/Round still running/i)).not.toBeInTheDocument()
  })
})
