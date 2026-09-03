import { useEffect, useRef, type RefObject } from 'react'

const dialogStack: symbol[] = []
const FOCUSABLE = [
  'button:not([disabled])',
  'summary',
  '[href]',
  'input:not([disabled])',
  'select:not([disabled])',
  'textarea:not([disabled])',
  '[tabindex]:not([tabindex="-1"])',
].join(', ')

function restoreFocus(opener: HTMLElement | null) {
  if (!opener?.isConnected) return
  opener.focus()
}

/**
 * One modal lifecycle for every overlay in the app.
 *
 * The module stack makes Escape and Tab belong only to the topmost mounted
 * dialog. The listener runs in capture phase so arena shortcuts never see an
 * Escape that closed a modal. Unmount restores the connected opener, including
 * a parent-dialog control when nested dialogs are introduced.
 */
export function useAccessibleDialog<T extends HTMLElement>(
  open: boolean,
  onClose: () => void,
): RefObject<T | null> {
  const surfaceRef = useRef<T | null>(null)
  const closeRef = useRef(onClose)
  const identityRef = useRef(Symbol('dialog'))
  const openerRef = useRef<HTMLElement | null>(null)

  useEffect(() => {
    closeRef.current = onClose
  }, [onClose])

  useEffect(() => {
    if (!open) return
    const identity = identityRef.current
    const surface = surfaceRef.current
    if (!surface) return
    const dialog: T = surface
    openerRef.current = document.activeElement instanceof HTMLElement
      ? document.activeElement
      : null
    dialogStack.push(identity)

    const focusables = () => Array.from(dialog.querySelectorAll<HTMLElement>(FOCUSABLE))
      .filter((node) => !node.hasAttribute('hidden') && node.getAttribute('aria-hidden') !== 'true')
    const initial = dialog.querySelector<HTMLElement>('[data-dialog-initial-focus]')
      ?? focusables()[0]
      ?? dialog
    if (initial === dialog && !dialog.hasAttribute('tabindex')) dialog.tabIndex = -1
    initial.focus()

    function handleKey(event: KeyboardEvent) {
      if (dialogStack.at(-1) !== identity) return
      if (event.key === 'Escape') {
        event.preventDefault()
        event.stopPropagation()
        event.stopImmediatePropagation()
        closeRef.current()
        return
      }
      if (event.key !== 'Tab') return
      const nodes = focusables()
      if (nodes.length === 0) {
        event.preventDefault()
        dialog.focus()
        return
      }
      const first = nodes[0]
      const last = nodes[nodes.length - 1]
      if (event.shiftKey && (document.activeElement === first || !dialog.contains(document.activeElement))) {
        event.preventDefault()
        last.focus()
      } else if (!event.shiftKey && (document.activeElement === last || !dialog.contains(document.activeElement))) {
        event.preventDefault()
        first.focus()
      }
    }

    document.addEventListener('keydown', handleKey, true)
    return () => {
      document.removeEventListener('keydown', handleKey, true)
      const index = dialogStack.lastIndexOf(identity)
      if (index >= 0) dialogStack.splice(index, 1)
      restoreFocus(openerRef.current)
    }
  }, [open])

  return surfaceRef
}
