/**
 * The share sheet, and what an operator is left holding when they dismiss it.
 *
 * The reported defect: pressing Cancel on the macOS share sheet left the user
 * with nothing -- no LinkedIn tab, no PNG, no caption -- because both share
 * buttons treated `AbortError` as a decision to abandon the post and returned
 * early. It is not that decision. It is a decision not to use *that* route, and
 * the card is already rendered by the time the sheet opens.
 *
 * Two things are pinned here. The decision itself, which is now one function in
 * `share.ts` rather than two copies of nine lines, and the shape of both call
 * sites, which is where the early return lived and where it could come back. The
 * second kind of assertion reads the source rather than the DOM because reaching
 * either button means rendering a card onto a canvas jsdom does not implement.
 */
import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { offerNativeShare } from './share'

const app = readFileSync(join(import.meta.dirname, 'App.tsx'), 'utf8')

/** A post the sheet would accept: caption, title and the rendered card. */
const POST: ShareData = {
  title: 'Lakebase: The Anti-Demo',
  text: 'Round 1 · the app woke in 41ms',
  files: [new File(['pixels'], 'card.png', { type: 'image/png' })],
}

/** Stand in for the OS sheet. `share` decides how the user answered it. */
function shareSheet(share: () => Promise<void>, canShare = true) {
  const shareSpy = vi.fn(share)
  vi.stubGlobal('navigator', {
    ...navigator,
    share: shareSpy,
    canShare: vi.fn(() => canShare),
  })
  return shareSpy
}

afterEach(() => {
  vi.unstubAllGlobals()
  vi.restoreAllMocks()
})

describe('the OS share sheet · what each answer means', () => {
  it('reports a post the OS accepted, and nothing more is owed', () => {
    // The only outcome where the operator already has what they wanted. Opening
    // LinkedIn and downloading a second copy on top of it would be noise.
    const sheet = shareSheet(() => Promise.resolve())
    return expect(offerNativeShare(POST)).resolves.toBe('shared').then(() => {
      expect(sheet).toHaveBeenCalledWith(POST)
    })
  })

  it('reports a dismissed sheet as dismissed rather than as a failure', async () => {
    // This is the reported bug's own path. `AbortError` is the only rejection
    // that means the user answered the sheet, so it is the only one that gets
    // its own outcome -- and it is not an error, because nothing failed.
    shareSheet(() => Promise.reject(new DOMException('Abort due to cancellation of share.', 'AbortError')))
    await expect(offerNativeShare(POST)).resolves.toBe('dismissed')
  })

  it('treats a sheet that broke as no sheet at all', async () => {
    // A NotAllowedError, a permissions policy, a share target that threw. The
    // user did not answer anything, so there is nothing to report to them about
    // the sheet -- the download path is simply the only path left.
    shareSheet(() => Promise.reject(new DOMException('Permission denied', 'NotAllowedError')))
    await expect(offerNativeShare(POST)).resolves.toBe('unavailable')

    shareSheet(() => Promise.reject(new TypeError('not a share target')))
    await expect(offerNativeShare(POST)).resolves.toBe('unavailable')
  })

  it('never opens a sheet that cannot carry the card', async () => {
    // Desktop Chrome answers `canShare` false for a file payload. Calling
    // `share` anyway is how a browser gets to reject with something this code
    // would then have to interpret.
    const sheet = shareSheet(() => Promise.resolve(), false)
    await expect(offerNativeShare(POST)).resolves.toBe('unavailable')
    expect(sheet).not.toHaveBeenCalled()
  })

  it('reports no sheet on a browser that has none', async () => {
    vi.stubGlobal('navigator', { ...navigator, share: undefined, canShare: undefined })
    await expect(offerNativeShare(POST)).resolves.toBe('unavailable')
  })
})

describe('both share buttons · a dismissal is not an abandonment', () => {
  /** The body of a share handler, from its outcome to the end of the function. */
  function handler(marker: string): string {
    const start = app.indexOf(marker)
    expect(start).toBeGreaterThan(0)
    const outcome = app.indexOf('const outcome = await offerNativeShare', start)
    expect(outcome).toBeGreaterThan(start)
    return app.slice(outcome, app.indexOf('\n  }\n', outcome))
  }

  // The receipt card and the six-round finale card. Two buttons, two screens,
  // one defect in each -- which is what made it worth sharing the decision.
  const SITES = [
    ['the receipt card', "title: 'Lakebase: The Anti-Demo',"],
    ['the finale card', "title: 'Lakebase: The Anti-Demo · Six-round finale',"],
  ] as const

  it('leaves exactly one early return in each handler, on the shared outcome', () => {
    for (const [name, marker] of SITES) {
      const body = handler(marker)
      expect(body, name).toContain("if (outcome === 'shared')")
      // One return: the one where the OS took the post. A second would be the
      // bug back again, because every other outcome owes the user a download.
      expect(body.match(/\breturn\b/g) ?? [], name).toHaveLength(1)
      expect(body, name).not.toMatch(/dismissed'\s*\)\s*return|=== 'dismissed'/)
    }
  })

  it('runs the download and the caption on every outcome the OS did not take', () => {
    for (const [name, marker] of SITES) {
      const body = handler(marker)
      // The three things the operator was left without: the tab, the PNG, the
      // caption. All three sit after the single return above.
      expect(body, name).toContain('window.open(')
      expect(body, name).toMatch(/download(?:ReceiptCard|FinaleCard)\(/)
      expect(body, name).toMatch(/cop(?:yPost|yCaption)\(/)
    }
  })

  it('says on screen that the sheet was dismissed, without stopping there', () => {
    for (const [name, marker] of SITES) {
      const body = handler(marker)
      // The status still has to account for the Cancel the operator just
      // pressed -- silently doing something else is its own confusion -- so
      // every status on the fallthrough path carries the prefix.
      expect(body, name).toContain('shareDismissalPrefix(outcome)')
      const statuses = [...body.matchAll(/set(?:Share)?Status\(([^\n]*)/g)].map(([, call]) => call)
      expect(statuses.length, name).toBeGreaterThan(1)
      for (const status of statuses.slice(1)) {
        expect(status, `${name}: ${status}`).toMatch(/dismissed/)
      }
    }
  })

  it('offers only what the screen it is on actually has', () => {
    // A copy bug found next to the defect: the receipt screen's last-resort
    // status told the operator to "use Download card", a control that screen
    // does not have, on the one path where the automatic download had failed.
    const shown = [...handler(SITES[0][1]).matchAll(/setStatus\(([^\n]*)/g)].map(([, call]) => call)
    expect(shown.length).toBeGreaterThan(1)
    // The comment above that branch names the retired wording, so this reads the
    // statuses themselves rather than the handler's prose.
    for (const status of shown) expect(status).not.toMatch(/Download card/)
    expect(shown.join('\n')).toMatch(/Screenshot this card/)
  })
})

describe('one spelling of the word for what the user just pressed', () => {
  // Both files, because the dismissal prefix lives in share.ts and everything
  // else it has to agree with lives in App.tsx.
  const copy = app + readFileSync(join(import.meta.dirname, 'share.ts'), 'utf8')

  /** Every string in either file that spells the word either way. */
  function occurrences(pattern: RegExp): string[] {
    return [...copy.matchAll(pattern)].map(([hit]) => hit)
  }

  it('spells it "cancelled" in the copy, and nowhere spells it "canceled"', () => {
    // Both spellings are correct English and the app was using both, sometimes
    // two lines apart. `cancelled` wins because every server-authored string
    // that reaches this screen already uses it, and copy that disagrees with
    // the payload beside it reads as two different products.
    expect(occurrences(/cancelled/g).length).toBeGreaterThan(1)
    expect(occurrences(/Share cancelled/g)).toHaveLength(1)
    const american = occurrences(/[Cc]anceled/g)
    expect(american).toEqual([])
  })

  it('leaves the wire alone, because the API is not copy', () => {
    // These are a contract with the server and with sessions already in flight,
    // so they are pinned verbatim rather than held to the copy decision above.
    // They happen to agree with it -- the server was the reason `cancelled` won
    // -- but agreement is not the reason they may not be touched.
    expect(occurrences(/'session_cancelled'/g)).toHaveLength(1)
    const identifiers = occurrences(/[a-z_]*cancel(?:led|l)?[a-z_]*/g)
      .filter((word) => word.includes('_'))
    expect(new Set(identifiers)).toEqual(new Set(['session_cancelled']))
  })
})
