// THE ILLEGIBLE FOOTER, PINNED SO IT CANNOT COME BACK.
//
// What was reported: on the fight card for a round the deployed app cannot run,
// the entire refusal paragraph was repeated across the bottom of the screen in
// type so small the lines overlapped each other and could not be read. It was a
// smear spanning the full width, under the two buttons, directly beneath a copy
// of the same prose the WHY panel was already showing in full.
//
// It was two defects wearing one coat. The wording is fixed in App.tsx, which no
// longer routes a 900-character reason to a status strip. This file covers the
// other one, which the wording fix does not touch: .game-lock-note could not
// survive being handed anything that wraps.
//
// WHY IT OVERSTRUCK. The strip is clamp(5px, .43vw, 8px) of "Press Start 2P",
// inherited from :root. That font reports a line box shorter than the glyphs it
// draws, so at the initial `normal` a second line lands on top of the first.
// Every other multi-line rule in this stylesheet sets a line-height explicitly
// for exactly that reason; this one never did, because every string routed to it
// was a one-line token like "LIVE UPDATES OFFLINE · LIVE PROOF LOCKED".
//
// WHY A GUARD AND NOT JUST THE FIX. The strip now has one occupant in App.tsx --
// `prepareRefusal`, the same value that greys the button out -- but two of the
// sentences that value can return are strings this repo does not write on the
// render path: a bout status's `maintenance_detail`, and whatever the round
// refusal carries. So "the text is short now" is a property of today's copy, not
// of the slot. The invariant is that the slot renders legibly whatever lands in
// it.
//
// WHY IT IS TESTED THIS WAY. jsdom does no layout and applies no stylesheet, so
// getComputedStyle would return nothing useful and a rendered assertion would
// pass against a broken sheet. The declaration is resolved out of the stylesheet
// text instead, the same way disclosure-contrast.test.tsx resolves colours.
// Comments are stripped first: this one names the very values it asserts on.

import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import { describe, expect, it } from 'vitest'

const css = readFileSync(join(import.meta.dirname, 'styles.css'), 'utf8')
  .replace(/\/\*[\s\S]*?\*\//g, '')

function bodyOf(selector: string): string | null {
  for (const [, selectors, body] of css.matchAll(/([^{}]+)\{([^{}]*)\}/g)) {
    const listed = selectors.split(',').map((one) => one.trim())
    if (listed.includes(selector)) return body
  }
  return null
}

function declaration(body: string, property: string): string | null {
  const found = new RegExp(`(?:^|[;{\\s])${property}\\s*:\\s*([^;}]+)`).exec(body)
  return found ? found[1].trim() : null
}

describe('the fight card status strip', () => {
  // Both rules that render prose at the pixel font's smallest sizes on this
  // screen. A third one pasted from either is the regression this covers.
  const WRAPPING = ['.game-lock-note', '.round-why-detail > summary']

  it('declares a line-height, because the pixel font overstrikes without one', () => {
    for (const selector of WRAPPING) {
      const body = bodyOf(selector)
      expect(body, `${selector} has no rule in styles.css`).not.toBeNull()
      const height = declaration(body!, 'line-height')
      expect(height, `${selector} leaves line-height at normal and will overstrike`).not.toBeNull()
      // 1.4 is the floor the rest of this stylesheet's multi-line rules sit at
      // or above, and comfortably clears the font's own glyph box.
      expect(Number(height), `${selector} line-height ${height} is too tight`).toBeGreaterThanOrEqual(1.4)
    }
  })

  it('keeps the strip off the full width of the cartridge', () => {
    // Full-bleed was half of what made the smear unreadable: a wrapped line
    // spanning the whole 16:9 frame at 5-8px has no measure at all.
    const body = bodyOf('.game-lock-note')!
    expect(declaration(body, 'max-width'), '.game-lock-note runs the full cartridge width').not.toBeNull()
  })

  it('breaks a long unbroken run rather than pushing the frame open', () => {
    // The strings it carries are not all written here, and one of them arriving
    // as a single long token would otherwise widen the grid row it sits in.
    const body = bodyOf('.game-lock-note')!
    expect(declaration(body, 'overflow-wrap')).toBe('anywhere')
  })
})
