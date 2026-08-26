// The roll must not scroll sideways, and the guard that stops it is structural.
//
// WHY THIS FILE EXISTS. At a 390px viewport the roll's frame is 250px wide and
// the roll inside it measured 332px -- 82px of horizontal overflow that clipped
// the rules card, the contribute link and the director's name. The cause was
// not any one of those three. The roll is a stack of single-column grids nested
// inside one another, and an implicit `auto` grid track takes its MINIMUM from
// the largest min-content contribution of its items, so one long run of text
// anywhere inside widens the track -- and every block, stretching to that
// track, then inherits the overflow. credits.css fixes it by pinning every one
// of those tracks with `minmax(0, 1fr)`.
//
// WHY IT IS TESTED THIS WAY. jsdom does no layout, so scrollWidth is always 0
// here and a width assertion would pass against a broken stylesheet. What can
// be checked is the invariant itself: every grid inside the roll is pinned.
// That is also the regression that will actually happen -- someone adds a block
// to the roll, or tidies the selector list, and the fix silently rots. The two
// sources of truth are cross-referenced rather than restated: the selectors
// come from credits.css and what counts as "inside the roll" comes from the
// rendered component, so neither a new CSS rule nor a new block in credits.tsx
// can slip past by being absent from a hand-written list here.
//
// Real pixel measurement is a browser's job and lives outside the unit suite.

import { readFileSync } from 'node:fs'
import { join } from 'node:path'
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

/**
 * The shipped stylesheet, comments stripped.
 *
 * Read off disk rather than imported: Vitest runs with `css: false`, so
 * `import './credits.css'` -- and `?raw` and `?inline` with it -- resolves to
 * an empty string, and every assertion below would pass against nothing.
 *
 * Stripping comments is not cosmetic either. The stylesheet documents this
 * very fix, so its prose contains both `minmax(0, 1fr)` and `text-overflow`.
 * Matching raw text would let the documentation satisfy the assertions that
 * the documentation describes.
 */
const css = readFileSync(join(import.meta.dirname, 'credits.css'), 'utf8')
  .replace(/\/\*[\s\S]*?\*\//g, '')

/**
 * The roll's OTHER stylesheet, and it has to be read here too.
 *
 * The authorship card is drawn inside the roll but styled from
 * credits-entry.css. The roll is split across two hand-authored sheets --
 * credits.css dresses the scene and everything that scrolls through it,
 * credits-entry.css dresses the card, the held mark and the entry control --
 * and that split must not become a hole in this guard: a grid inside the
 * roll is exactly as able to widen the whole roll whichever sheet dressed it.
 */
const entryCss = readFileSync(join(import.meta.dirname, 'credits-entry.css'), 'utf8')
  .replace(/\/\*[\s\S]*?\*\//g, '')

/** Flat `selector { body }` pairs. Rules nested in @media fall out on their own. */
const parse = (sheet: string) => [...sheet.matchAll(/([^{}]+)\{([^{}]*)\}/g)].map(([, selector, body]) => ({
  selectors: selector
    .split(',')
    .map((one: string) => one.trim())
    .filter((one: string) => one.length > 0 && !one.startsWith('@')),
  body,
}))

const rules = [...parse(css), ...parse(entryCss)]

const declares = (pattern: RegExp) =>
  rules.filter((rule) => pattern.test(rule.body)).flatMap((rule) => rule.selectors)

afterEach(cleanup)

describe('the roll cannot be widened past its frame', () => {
  it('pins the grid track of every grid inside the roll', () => {
    const { container } = roll()
    const rollNode = container.querySelector('.credits-roll')
    expect(rollNode).not.toBeNull()

    /** Does this selector match the roll itself, or anything the roll contains? */
    const insideRoll = (selector: string) => {
      let matched: Element[]
      try {
        matched = [...container.querySelectorAll(selector)]
      } catch {
        return false
      }
      return matched.some((node) => node === rollNode || rollNode!.contains(node))
    }

    const pinned = new Set(declares(/grid-template-columns:\s*minmax\(\s*0\s*,\s*1fr\s*\)/))
    const grids = declares(/display:\s*grid/).filter(insideRoll)

    // Sanity: if this ever empties, the cross-reference has broken and the
    // assertion below would pass vacuously.
    expect(grids.length).toBeGreaterThan(5)
    expect(grids.filter((selector) => !pinned.has(selector))).toEqual([])
  })

  it('pins the blocks that actually overflowed, by name', () => {
    // Belt and braces on the ones the bug report named, so a future refactor
    // that renames a class cannot quietly drop one of them.
    //
    // `.credits-finale > div` is NOT on this list any more, and its absence is
    // the assertion. It stayed here after the app stopped rendering it only
    // because the standalone Desktop build still did; that build has now been
    // ported to the same framed `.production-credit` card, so the class is dead
    // in both consumers and its rules are gone from credits.css. What
    // replaced it is pinned below, and that pin is what actually matters: the
    // author credit's name is the widest string on the roll.
    const pinned = new Set(declares(/grid-template-columns:\s*minmax\(\s*0\s*,\s*1fr\s*\)/))
    for (const selector of [
      '.credits-roll',
      '.credits-rules',
      '.credits-invite',
      // The card that replaced the finale, styled from the hand-authored sheet.
      '.production-credit > div',
    ]) {
      expect(pinned).toContain(selector)
    }
  })
})

describe('the authorship card', () => {
  it('never truncates the name, it wraps it', () => {
    // The name is a credit. A clipped author credit is worse than a two-line
    // one, so the wrap must come from overflow-wrap and never from an ellipsis.
    // Checked against BOTH sheets: the card moved between them.
    for (const sheet of [css, entryCss]) {
      expect(sheet).not.toMatch(/text-overflow/)
      expect(sheet).not.toMatch(/line-clamp/)
    }
    expect(declares(/overflow-wrap:\s*break-word/)).toContain('.production-credit strong')
  })

  it('lets the display line wrap, because Press Start 2P is wider per character', () => {
    // The card inherited its metrics from the removed app-wide footer, which
    // set `white-space: nowrap` on a 6-10px line. At display size, in a face
    // far wider per character than the ui-monospace it used to fall back to, a
    // line that cannot wrap is how a 390px overflow gets reintroduced.
    const card = entryCss.match(/\.production-credit\s*\{([^}]*)\}/)
    expect(card).not.toBeNull()
    expect(card![1]).not.toMatch(/white-space:\s*nowrap/)
    expect(entryCss).not.toMatch(/white-space:\s*nowrap/)
    expect(css).not.toMatch(/white-space:\s*nowrap/)
  })

  it('keeps the portrait beside the name above the narrow breakpoint', () => {
    const { container } = roll()

    // Source order is the layout: img first, then the text column, laid out by
    // a row flexbox. Only the sub-640px fallback turns that into a column.
    const card = container.querySelector('.production-credit')
    expect(card).not.toBeNull()
    expect(card!.children[0].tagName).toBe('IMG')
    expect(card!.children[1].tagName).toBe('DIV')

    const stacked = entryCss.match(/@media\s*\(max-width:\s*640px\)\s*\{([\s\S]*?)\n\}/)
    expect(stacked?.[1]).toMatch(/\.production-credit\s*\{[^}]*flex-direction:\s*column/)
  })

  it('is framed like the other beats on the roll', () => {
    // What "just normal text" was actually describing: every neighbouring beat
    // sits in a bordered --panel box with an inset accent and a hard shadow,
    // and this one had no container at all. Square, per the house rule.
    const card = entryCss.match(/\.production-credit\s*\{([^}]*)\}/)![1]
    expect(card).toMatch(/border:\s*clamp\([^)]*\)\s+solid\s+var\(--cream\)/)
    expect(card).toMatch(/background:\s*var\(--panel\)/)
    expect(card).toMatch(/box-shadow:\s*inset/)
    expect(card).not.toMatch(/border-radius/)
  })
})
