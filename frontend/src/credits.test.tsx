// What is on screen in the roll, tested on <Credits> directly.
//
// credits-entry.test.tsx already covers the door -- the portal, the exit, the
// focus round trip and the music policy -- so this file only asserts the
// content of the roll itself: that the contribute repo is a live, reachable
// link, that the cut act list stays cut, and that the finale card bills the
// portrait beside the name rather than stacked above it.

import { act, cleanup, render, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { Credits, CONTRIBUTE_REPO, CONTRIBUTE_URL } from './credits'
import { CREDITS_THEME_BELL_SECONDS } from './credits-score'
import type { CreditsTally } from './credits-tally'
import type { CompetitorDefinition } from './api/types'

/* Both paths matter here. The roll is animated, so "is the link clickable" has
   a different answer while it is crawling than it does under reduced motion,
   and the static variant is the one that has to keep pointer-events. */
const motion = vi.hoisted(() => ({ reduced: true }))
vi.mock('./hooks/useReducedMotion', () => ({ useReducedMotion: () => motion.reduced }))

const audioMocks = vi.hoisted(() => ({
  playConfirm: vi.fn(),
  playCursor: vi.fn(),
  playOriginalBell: vi.fn(),
  startOriginalCreditsTheme: vi.fn(() => false),
  startOriginalTitleTheme: vi.fn(),
  stopOriginalCreditsTheme: vi.fn(),
  stopOriginalTitleTheme: vi.fn(() => false),
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

const tally: CreditsTally = {
  bouts: 1,
  lakebaseWins: 1,
  uncontested: 0,
  abandoned: 0,
  competitors: ['Aurora'],
}

function roll() {
  return render(
    <Credits competitors={competitors} tally={tally} sound={false} onBack={() => {}} />,
  )
}

beforeEach(() => {
  motion.reduced = true
  Object.values(audioMocks).forEach((mock) => mock.mockClear())
})

afterEach(cleanup)

describe('the contribute invitation', () => {
  it('bills the repo as a real link to GitHub', () => {
    roll()

    const link = screen.getByRole('link', { name: CONTRIBUTE_REPO })
    expect(link).toHaveAttribute('href', 'https://github.com/ryancicak/lakebase-anti-demo')
    expect(link).toHaveAttribute('href', CONTRIBUTE_URL)
  })

  it('shows the bare repo path, not the scheme', () => {
    roll()

    const link = screen.getByRole('link', { name: CONTRIBUTE_REPO })
    expect(link).toHaveTextContent('github.com/ryancicak/lakebase-anti-demo')
    expect(link.textContent).not.toContain('https://')
  })

  it('opens in a new tab without handing the opener over', () => {
    roll()

    const link = screen.getByRole('link', { name: CONTRIBUTE_REPO })
    // The roll is an overlay over a possibly-live bout; navigating this tab away
    // would take the run with it.
    expect(link).toHaveAttribute('target', '_blank')
    expect(link.getAttribute('rel')).toContain('noopener')
    expect(link.getAttribute('rel')).toContain('noreferrer')
  })

  it('sits under the invitation kicker, not on the authorship card', () => {
    const { container } = roll()

    const invite = container.querySelector('.credits-invite')
    expect(invite).not.toBeNull()
    expect(within(invite as HTMLElement).getByRole('link', { name: CONTRIBUTE_REPO }))
      .toBeInTheDocument()
    expect(container.querySelector('.production-credit a')).toBeNull()
  })

  it('keeps exactly one invitation and one repo link on the roll', () => {
    // The hook moved to the top; it was not copied there. Two calls to action
    // would also mean two long URLs to keep from overflowing at 390px.
    const { container } = roll()

    expect(container.querySelectorAll('.credits-invite')).toHaveLength(1)
    expect(screen.getAllByRole('link', { name: CONTRIBUTE_REPO })).toHaveLength(1)
  })

  it('keeps a break opportunity at every slash and hyphen', () => {
    // The <wbr>s travelled with the markup when it moved. Without them the repo
    // is one 39-character word and breaks mid-handle at 390px.
    const { container } = roll()

    const link = container.querySelector('.credits-invite-link') as HTMLElement
    expect(link.querySelectorAll('wbr').length).toBe(CONTRIBUTE_REPO.split(/(?<=[/-])/).length - 1)
    // The <wbr>s are childless, so the accessible name is still the bare repo.
    expect(link).toHaveAccessibleName(CONTRIBUTE_REPO)
  })
})

/* THE ORDER IS THE POINT.
 *
 * "call out the github PR hook at the TOP - so every contributor is seen! its
 * not just the Ryan Cicak show". Asserting the hook merely EXISTS is what the
 * suite did before and it passed against the old layout, where the invitation
 * sat ninth of ten between the fairness card and the author credit. These
 * assertions are about position, and each one fails against that layout. */
describe('the running order', () => {
  function blocks(container: HTMLElement) {
    return Array.from(container.querySelector('.credits-roll')!.children)
  }

  it('leads with the contribute hook, ahead of every other beat', () => {
    const { container } = roll()
    const order = blocks(container)

    // Title card, then the invitation. Nothing between them.
    expect(order[0].className).toContain('credits-title')
    expect(order[1].className).toContain('credits-invite')

    const invite = order.findIndex((node) => node.classList.contains('credits-invite'))
    for (const behind of ['credits-crew', 'credits-block', 'credits-rules', 'production-credit']) {
      const first = order.findIndex((node) => node.classList.contains(behind))
      expect(first, `${behind} must fall behind the invitation`).toBeGreaterThan(invite)
    }
  })

  it('bills the crew directly behind the invitation', () => {
    // The door and the people who have walked through it read as one unit, and
    // a contributor still reaches their own name before any match content.
    const { container } = roll()
    const order = blocks(container)

    expect(order[2].className).toContain('credits-crew')
    expect(within(order[2] as HTMLElement).getByText('Built by')).toBeInTheDocument()
  })

  it('puts the contribute link ahead of the author credit in document order', () => {
    const { container } = roll()

    const link = screen.getByRole('link', { name: CONTRIBUTE_REPO })
    // Scoped to the crawl deliberately. Against the old layout the author was
    // also named on the held card, which trivially follows the link wherever
    // the link sits -- so an unscoped query would pass without the hook having
    // moved at all.
    const credit = container.querySelector('.credits-roll .production-credit strong') as HTMLElement
    expect(credit).not.toBeNull()
    // Following, not merely different: the hook is read first.
    expect(link.compareDocumentPosition(credit) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()
  })

  it('closes the roll on the author credit, not on the hook', () => {
    // The invitation leading does not mean the roll ends on nothing. The
    // authorship card is still the last beat -- it just is not the only beat
    // the roll was building toward.
    const { container } = roll()
    const order = blocks(container)

    expect(order[order.length - 1].className).toContain('production-credit')
  })

  it('is keyboard reachable and takes focus', async () => {
    const user = userEvent.setup()
    roll()

    const link = screen.getByRole('link', { name: CONTRIBUTE_REPO })
    // An anchor with an href is in the tab order, which is also what
    // CreditsButton's focus trap enumerates ('[href]').
    await user.tab()
    for (let hop = 0; hop < 8 && document.activeElement !== link; hop += 1) {
      await user.tab()
    }
    expect(link).toHaveFocus()
  })

  it('does not dismiss the roll when the link is followed mid-crawl', async () => {
    motion.reduced = false
    const user = userEvent.setup()
    const { container } = roll()

    const frame = container.querySelector('.credits-frame') as HTMLElement
    expect(frame).toHaveAttribute('data-rolled', 'false')

    const link = screen.getByRole('link', { name: CONTRIBUTE_REPO })
    await user.click(link)

    // The frame's own click handler skips the roll. The anchor stops the click
    // from reaching it, so following the link does not fade out the thing the
    // viewer was reading -- and the frame keeps its pointer-events.
    expect(frame).toHaveAttribute('data-rolled', 'false')

    // A click anywhere else in the frame still skips, so the guard is on the
    // anchor rather than on the frame.
    await user.click(frame)
    expect(frame).toHaveAttribute('data-rolled', 'true')
  })

  it('keeps the static roll interactive under reduced motion', () => {
    const { container } = roll()

    // Reduced motion starts in the rolled state, and the static treatment is
    // what restores opacity and pointer-events so the link stays usable.
    const frame = container.querySelector('.credits-frame') as HTMLElement
    expect(frame).toHaveAttribute('data-static', 'true')
    expect(frame).toHaveAttribute('data-rolled', 'true')
  })
})

describe('the cut act list', () => {
  it('does not bill "The card"', () => {
    roll()

    expect(screen.queryByText('The card')).not.toBeInTheDocument()
  })

  it('leaves no act-list markup behind', () => {
    const { container } = roll()

    expect(container.querySelector('.credits-card-list')).toBeNull()
    expect(container.querySelector('ol')).toBeNull()
  })

  it('bills none of the six rounds', () => {
    roll()

    for (const title of [
      'Wake this idle app',
      'Make this schema change safely',
      'Recover this deleted order',
      'Move lakehouse data into live applications',
      'Get spike-ready',
      'Move live application data into the lakehouse',
    ]) {
      expect(screen.queryByText(title)).not.toBeInTheDocument()
    }
    expect(screen.queryByText(/^Round \d$/)).not.toBeInTheDocument()
  })

  it('keeps the blocks either side of it', () => {
    roll()

    expect(screen.getByText('Built by')).toBeInTheDocument()
    expect(screen.getByText('In the red corner')).toBeInTheDocument()
  })
})

/* The block that was cut shortened the roll, and a shorter roll at a fixed
   speed lands before the score does. jsdom has no layout, so the frame and roll
   heights are stubbed and the frames are pumped by hand: what is being asserted
   is the arithmetic that decides when the roll ends, not the rendering. */
describe('the roll clock', () => {
  const FRAME_HEIGHT = 800
  /* Travel of 2000px is 50s at the old fixed 40 px/s -- comfortably before the
     cue's bell, which is the failure the retune exists to prevent. */
  const ROLL_HEIGHT = 1200
  const TRAVEL = ROLL_HEIGHT + FRAME_HEIGHT
  const OLD_FIXED_SPEED_MS = (TRAVEL / 40) * 1000

  let frames: FrameRequestCallback[] = []
  const restore: Array<() => void> = []

  function stubLayout() {
    const client = vi.spyOn(Element.prototype, 'clientHeight', 'get')
      .mockImplementation(function (this: Element) {
        return this.classList.contains('credits-frame') ? FRAME_HEIGHT : 0
      })
    const scroll = vi.spyOn(Element.prototype, 'scrollHeight', 'get')
      .mockImplementation(function (this: Element) {
        return this.classList.contains('credits-roll') ? ROLL_HEIGHT : 0
      })
    const raf = vi.spyOn(window, 'requestAnimationFrame')
      .mockImplementation((callback: FrameRequestCallback) => {
        frames.push(callback)
        return frames.length
      })
    restore.push(() => client.mockRestore(), () => scroll.mockRestore(), () => raf.mockRestore())
  }

  /** Runs the most recently requested frame at `now`. */
  function pump(now: number) {
    const next = frames.pop()
    if (!next) throw new Error('the roll requested no frame')
    act(() => { next(now) })
  }

  beforeEach(() => {
    motion.reduced = false
    frames = []
    stubLayout()
  })

  afterEach(() => {
    restore.splice(0).forEach((undo) => undo())
  })

  function rollWithCue() {
    audioMocks.startOriginalCreditsTheme.mockReturnValue(true)
    return render(
      <Credits competitors={competitors} tally={tally} sound onBack={() => {}} />,
    )
  }

  it('starts one frame height down and crawls up', () => {
    const { container } = rollWithCue()
    const track = container.querySelector('.credits-roll') as HTMLElement

    pump(0)
    // translateY(100%) would be 100% of the ROLL's height, which is the
    // double-speed bug the keyframe comment warns about. This is the frame's.
    expect(track.style.transform).toBe(`translateY(${FRAME_HEIGHT}px)`)
  })

  it('is still crawling at the point the old fixed speed would have ended', () => {
    const { container } = rollWithCue()
    const frame = container.querySelector('.credits-frame') as HTMLElement

    pump(0)
    pump(OLD_FIXED_SPEED_MS)

    // The cue holds the transport, so the picture is cut to the music: at 50s
    // the score has 35s left to play and the roll must not have ended.
    expect(frame).toHaveAttribute('data-rolled', 'false')
    expect(audioMocks.playOriginalBell).not.toHaveBeenCalled()
  })

  it('lands on the cue bell, and rings it there', () => {
    const { container } = rollWithCue()
    const frame = container.querySelector('.credits-frame') as HTMLElement
    const track = container.querySelector('.credits-roll') as HTMLElement

    pump(0)
    pump(CREDITS_THEME_BELL_SECONDS * 1000)

    expect(track.style.transform).toBe(`translateY(${-ROLL_HEIGHT}px)`)
    expect(frame).toHaveAttribute('data-rolled', 'true')
    expect(audioMocks.playOriginalBell).toHaveBeenCalledTimes(1)
  })

  it('falls back to the reference speed when there is no cue to sync to', () => {
    audioMocks.startOriginalCreditsTheme.mockReturnValue(false)
    const { container } = render(
      <Credits competitors={competitors} tally={tally} sound={false} onBack={() => {}} />,
    )
    const frame = container.querySelector('.credits-frame') as HTMLElement

    pump(0)
    pump(OLD_FIXED_SPEED_MS)

    // Sound was off when the roll opened, so there is no music to land with and
    // 40 px/s is the right answer.
    expect(frame).toHaveAttribute('data-rolled', 'true')
  })

  it('runs no clock at all under reduced motion', () => {
    motion.reduced = true
    render(<Credits competitors={competitors} tally={tally} sound={false} onBack={() => {}} />)

    expect(frames).toHaveLength(0)
  })
})

/* ONE AUTHORSHIP MOMENT, NOT TWO.
 *
 * The roll used to bill the portrait and "A Ryan Cicak Production" inside the
 * crawl AND print the same six words again on the held card seconds later. Two
 * identical claims is the arithmetic behind "not just the Ryan Cicak show", so
 * the count is what these assertions pin, not just the presence. */
describe('the authorship card', () => {
  it('claims authorship exactly once, on the card that owns it', () => {
    const { container } = roll()

    const claims = Array.from(container.querySelectorAll('*'))
      .filter((node) => node.children.length === 0
        && /A Ryan Cicak Production/i.test(node.textContent ?? ''))
    expect(claims).toHaveLength(1)
    expect(claims[0].closest('.production-credit')).not.toBeNull()
  })

  it('names the author twice on the whole roll: as a contributor, and as the author', () => {
    // Deliberately not zero. He wrote it and is still credited -- once in the
    // crew list he genuinely belongs in, once as the author. It used to be four
    // counting the duplicate claim.
    const { container } = roll()

    const mentions = Array.from(container.querySelectorAll('*'))
      .filter((node) => node.children.length === 0
        && /Ryan Cicak/i.test(node.textContent ?? ''))
    expect(mentions).toHaveLength(2)
    expect(mentions.filter((node) => node.closest('.credits-crew'))).toHaveLength(1)
    expect(mentions.filter((node) => node.closest('.production-credit'))).toHaveLength(1)
  })

  it('pairs the portrait with the name rather than stacking it above', () => {
    const { container } = roll()

    const card = container.querySelector('.production-credit') as HTMLElement
    expect(card).not.toBeNull()

    const [portrait, billing] = Array.from(card.children)
    expect(portrait.tagName).toBe('IMG')
    // The name and its caption are wrapped as one column beside the portrait,
    // which is what lets them sit vertically centered against it. Only the
    // sub-640px fallback in credits-entry.css turns that into a column.
    expect(billing.tagName).toBe('DIV')
    expect(within(billing as HTMLElement).getByText('A Ryan Cicak Production'))
      .toBeInTheDocument()
    expect(within(billing as HTMLElement).getByText('Thanks for stepping into the ring'))
      .toBeInTheDocument()
  })

  it('uses the pixel sprite the user approved, not the old photograph', () => {
    const { container } = roll()

    // The 256px photo it replaced never sat right beside a roll drawn entirely
    // on the NES grid; the sprite is 64x64 on the app's own four tokens.
    const portrait = container.querySelector('.production-credit img') as HTMLImageElement
    expect(portrait.getAttribute('src')).toMatch(/ryan-pixel-portrait/)
    expect(portrait.getAttribute('src')).not.toMatch(/ryan-ringside/)
    // Decorative: the name next to it is the accessible text.
    expect(portrait).toHaveAttribute('alt', '')
  })

  it('is the only place the portrait appears', () => {
    const { container } = roll()

    const portraits = Array.from(container.querySelectorAll('img'))
      .filter((image) => (image.getAttribute('src') ?? '').includes('ryan-pixel'))
    expect(portraits).toHaveLength(1)
    expect(portraits[0].closest('.production-credit')).not.toBeNull()
  })

  it('leaves no finale card behind', () => {
    // The class survives in credits.css only as the removal note that records
    // why it went; nothing in the app may render it.
    const { container } = roll()

    expect(container.querySelector('.credits-finale')).toBeNull()
  })

  it('bills no unlabelled likeness of anyone else', () => {
    const { container } = roll()

    for (const image of Array.from(container.querySelectorAll('img'))) {
      expect(image.getAttribute('src')).not.toMatch(/team-member|personas-ringside/)
    }
  })
})

describe('the held card after the crawl', () => {
  it('is not rendered at all under reduced motion', () => {
    // `rolled` is initialised to reducedMotion, so this used to render straight
    // away there -- and being position:absolute/inset:0 it landed on top of the
    // static list, striking through the crew card. There is no crawl under
    // reduced motion, so there is no "after the crawl" for a held card.
    const { container } = roll()

    const frame = container.querySelector('.credits-frame') as HTMLElement
    expect(frame).toHaveAttribute('data-static', 'true')
    expect(frame).toHaveAttribute('data-rolled', 'true')
    expect(container.querySelector('.credits-outro')).toBeNull()
  })

  it('holds the project mark once the crawl has actually ended', async () => {
    motion.reduced = false
    const user = userEvent.setup()
    const { container } = roll()

    await user.click(container.querySelector('.credits-frame') as HTMLElement)

    const outro = container.querySelector('.credits-outro') as HTMLElement
    expect(outro).not.toBeNull()
    expect(within(outro).getByText('Lakebase')).toBeInTheDocument()
    expect(within(outro).getByText('The Anti-Demo')).toBeInTheDocument()
    // The two coloured rules either side of the name are the one piece of the
    // old production line worth keeping here.
    expect(outro.querySelectorAll('.credits-outro-title > span')).toHaveLength(2)
  })

  it('does not claim authorship a second time', async () => {
    motion.reduced = false
    const user = userEvent.setup()
    const { container } = roll()

    await user.click(container.querySelector('.credits-frame') as HTMLElement)

    const outro = container.querySelector('.credits-outro') as HTMLElement
    expect(outro.textContent).not.toMatch(/Ryan Cicak/i)
    expect(outro.textContent).not.toMatch(/Production/i)
    expect(container.querySelector('.credits-director')).toBeNull()
  })

  it('repeats no portrait', async () => {
    motion.reduced = false
    const user = userEvent.setup()
    const { container } = roll()

    await user.click(container.querySelector('.credits-frame') as HTMLElement)

    const outro = container.querySelector('.credits-outro') as HTMLElement
    const portraits = Array.from(outro.querySelectorAll('img'))
      .filter((image) => (image.getAttribute('src') ?? '').includes('ryan-pixel'))
    expect(portraits).toHaveLength(0)
  })
})
