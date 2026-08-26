// The way into the credits roll, and the way back out.
//
// Everything about opening and closing lives here: the trigger button, the
// portal, the Escape / B exit, the focus round trip, the focus trap, and the
// music policy (which is: touch nothing). credits.tsx owns only what is drawn
// on screen once it is open.
//
// This replaces an earlier design in which the "A RYAN CICAK PRODUCTION"
// footer was itself the trigger. That footer rendered from fifteen places
// across the app and read as sole ownership of a project meant to take
// contributions, so it was removed outright. The roll it opened was worth
// keeping, so the mechanism survives with a different door: see CreditsButton.

import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from 'react'
import { createPortal } from 'react-dom'
import { Credits } from './credits'
import { creditsTally, type CreditsScorecardEntry } from './credits-tally'
import { playConfirm, playCursor } from './audio'
import { useReducedMotion } from './hooks/useReducedMotion'
import type { CompetitorDefinition } from './api/types'
import './credits-entry.css'

/**
 * What the roll needs to know.
 *
 * Passed as one object rather than four props because it crosses two screen
 * components that otherwise have no interest in any of it. App builds it once,
 * memoized, next to its other derived values.
 */
export interface CreditsEntry {
  /** `catalog.competitors`. The roll bills only the ones the tally names. */
  competitors: CompetitorDefinition[]
  /** App's `scorecard`. Reduced to a tally by the roll; never mutated. */
  scorecard: CreditsScorecardEntry[]
  /** App's `sound`. Gates the two one-shot blips and the finale bell. */
  sound: boolean
  /**
   * True while a bout is actually running.
   *
   * This does NOT disable anything. It suppresses the finale bell -- a bell is
   * the app's own signal that a round ended, and ringing one over a live run
   * would be a false cue -- and it puts a standing note on the roll so nobody
   * watching the credits thinks the demo finished.
   *
   * Deliberately narrower than "a session exists": a verified, towelled or
   * failed session is finished and the note would be a lie.
   */
  boutInFlight: boolean
}

export function CreditsButton({
  entry,
  className,
  children,
}: {
  entry: CreditsEntry
  /** Placement is the caller's business; this component only owns behaviour. */
  className: string
  children: ReactNode
}) {
  const [open, setOpen] = useState(false)
  const triggerRef = useRef<HTMLButtonElement | null>(null)
  const surfaceRef = useRef<HTMLDivElement | null>(null)
  /* Drives the attract blink off, following the roll's own convention: credits.tsx
     puts the same hook behind `data-static` rather than relying on the media
     query alone, which keeps the behaviour assertable in jsdom. */
  const reducedMotion = useReducedMotion()

  const { sound } = entry

  /** Close, and hand focus back to the button that opened the roll.
   *
   *  rAF because the portal is still mounted on this tick; focusing after the
   *  commit means the button is back in the layout when it receives focus. */
  const close = useCallback(() => {
    setOpen(false)
    if (sound) playCursor()
    requestAnimationFrame(() => triggerRef.current?.focus())
  }, [sound])

  const openRoll = useCallback(() => {
    if (sound) playConfirm()
    setOpen(true)
  }, [sound])

  /**
   * Escape closes.
   *
   * Deliberately the same shape as ApiIndicator's `closeOnEscape` in App.tsx: a
   * window listener mounted only while the thing is open, torn down with it,
   * restoring focus to the trigger ref. Following the existing convention
   * rather than adding a competing always-on handler is also what keeps this
   * from firing while the credits are closed, so it can never swallow an
   * Escape the arena wanted.
   */
  useEffect(() => {
    if (!open) return
    function closeOnEscape(event: KeyboardEvent) {
      if (event.key !== 'Escape') return
      event.preventDefault()
      setOpen(false)
      requestAnimationFrame(() => triggerRef.current?.focus())
    }
    window.addEventListener('keydown', closeOnEscape)
    return () => window.removeEventListener('keydown', closeOnEscape)
  }, [open])

  /**
   * Move focus into the roll, and keep Tab inside it.
   *
   * The screen underneath is still mounted and still live -- that is the whole
   * point -- but it is covered, so tabbing into controls nobody can see would
   * be worse than trapping. The trap only ever reads the overlay's own
   * focusables and never touches the app tree, so it cannot disturb a run.
   */
  useEffect(() => {
    if (!open) return
    const surface = surfaceRef.current
    if (!surface) return

    const focusables = () => Array.from(
      surface.querySelectorAll<HTMLElement>('button:not([disabled]), [href], [tabindex]:not([tabindex="-1"])'),
    )
    // The exit is the first thing a keyboard user should land on.
    const first = surface.querySelector<HTMLElement>('.credits-back') ?? focusables()[0]
    first?.focus()

    function keepFocusInside(event: KeyboardEvent) {
      if (event.key !== 'Tab') return
      const nodes = focusables()
      if (nodes.length === 0) return
      const edge = event.shiftKey ? nodes[0] : nodes[nodes.length - 1]
      if (document.activeElement === edge || !surface!.contains(document.activeElement)) {
        event.preventDefault()
        ;(event.shiftKey ? nodes[nodes.length - 1] : nodes[0]).focus()
      }
    }
    surface.addEventListener('keydown', keepFocusInside)
    return () => surface.removeEventListener('keydown', keepFocusInside)
  }, [open])

  const tally = useMemo(() => creditsTally(entry.scorecard), [entry.scorecard])

  return (
    <>
      {/* The caret is a menu cursor, not decoration, so it belongs to the control
          rather than to either caller: both entry points get the same pointer in
          the same place. It is aria-hidden and its box is always reserved, so
          revealing it on hover and focus never nudges the label. */}
      <button
        ref={triggerRef}
        className={className}
        type="button"
        title="Play the staff roll"
        data-static={reducedMotion}
        aria-haspopup="dialog"
        aria-expanded={open}
        onClick={openRoll}
      >
        <span className="credits-entry-cursor" aria-hidden="true">▶</span>
        <span className="credits-entry-label">{children}</span>
      </button>

      {/* Portalled to <body> rather than rendered in place. Two reasons: the
          triggers sit inside overflow:hidden 16:9 boxes, so a position:fixed
          child of one would be clipped; and portalling makes it unambiguous
          that the roll is rendered IN ADDITION TO the current screen, never
          instead of it. The screen underneath stays mounted, so nothing about
          an in-flight bout or its event stream is disturbed. */}
      {open && createPortal(
        <div
          className="credits-portal"
          ref={surfaceRef}
          role="dialog"
          aria-modal="true"
          aria-label="Credits"
        >
          <Credits
            competitors={entry.competitors}
            tally={tally}
            sound={entry.sound}
            boutInFlight={entry.boutInFlight}
            onBack={close}
          />
        </div>,
        document.body,
      )}
    </>
  )
}
