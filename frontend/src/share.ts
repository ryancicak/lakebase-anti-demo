/**
 * The OS share sheet, and what an operator is left holding when they dismiss it.
 *
 * Its own module for the reason `time.ts` and `recap.ts` are: it is a pure
 * decision with no React in it, and App.tsx is the one file in this app whose
 * fast refresh a non-component export would switch off.
 */

/** Whether the OS took the post, the user dismissed the sheet, or there is no sheet. */
export type NativeShareOutcome = 'shared' | 'dismissed' | 'unavailable'

/**
 * Offer the post to the OS share sheet, and never let that offer be the only one.
 *
 * WHY THIS IS A FUNCTION AND NOT TWO COPIES. There are two share buttons -- one
 * per receipt and one for the six-round card -- and both had the same nine lines
 * inlined, including the same bug: `dismissed` returned early, which skipped
 * opening LinkedIn, downloading the PNG *and* copying the caption. Dismissing
 * the macOS share sheet is not a decision to abandon the post, so it now falls
 * through to the download path like every other non-share outcome.
 *
 * Only the decision lives here. The status copy stays at each call site, because
 * the two screens do not offer the same things and one wording that covered both
 * would be vaguer than either. `unavailable` folds in a non-abort rejection,
 * which is what both call sites already did with one: a share target that threw
 * is a route that did not work, not an answer from the user.
 */
export async function offerNativeShare(shareData: ShareData): Promise<NativeShareOutcome> {
  const canShareFile = typeof navigator.share === 'function'
    && typeof navigator.canShare === 'function'
    && navigator.canShare(shareData)
  if (!canShareFile) return 'unavailable'
  try {
    await navigator.share(shareData)
    return 'shared'
  } catch (error) {
    return error instanceof DOMException && error.name === 'AbortError'
      ? 'dismissed'
      : 'unavailable'
  }
}

/** Prefix that keeps a dismissed sheet visible in whatever happened next. */
export function shareDismissalPrefix(outcome: NativeShareOutcome): string {
  return outcome === 'dismissed' ? 'Share cancelled · ' : ''
}
