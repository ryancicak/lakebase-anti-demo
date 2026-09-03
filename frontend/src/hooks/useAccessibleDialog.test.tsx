import { useState } from 'react'
import { cleanup, render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { useAccessibleDialog } from './useAccessibleDialog'

function NestedDialogs() {
  const [parentOpen, setParentOpen] = useState(false)
  const [childOpen, setChildOpen] = useState(false)
  const parentRef = useAccessibleDialog<HTMLDivElement>(parentOpen, () => setParentOpen(false))
  const childRef = useAccessibleDialog<HTMLDivElement>(childOpen, () => setChildOpen(false))
  return (
    <>
      <button onClick={() => setParentOpen(true)}>Open parent</button>
      {parentOpen && (
        <div ref={parentRef} role="dialog" aria-modal="true" aria-label="Parent">
          <button onClick={() => setChildOpen(true)}>Open child</button>
          <button onClick={() => setParentOpen(false)}>Close parent</button>
          {childOpen && (
            <div ref={childRef} role="dialog" aria-modal="true" aria-label="Child">
              <button data-dialog-initial-focus onClick={() => setChildOpen(false)}>Close child</button>
              <a href="#child-proof">Child proof</a>
            </div>
          )}
        </div>
      )}
    </>
  )
}

describe('useAccessibleDialog', () => {
  afterEach(cleanup)

  it('traps focus, restores the opener, and closes only the topmost dialog', async () => {
    const user = userEvent.setup()
    render(<NestedDialogs />)
    const rootOpener = screen.getByRole('button', { name: 'Open parent' })

    await user.click(rootOpener)
    const childOpener = screen.getByRole('button', { name: 'Open child' })
    expect(childOpener).toHaveFocus()
    await user.keyboard('{Shift>}{Tab}{/Shift}')
    expect(screen.getByRole('button', { name: 'Close parent' })).toHaveFocus()
    await user.keyboard('{Tab}')
    expect(childOpener).toHaveFocus()

    await user.click(childOpener)
    const childClose = screen.getByRole('button', { name: 'Close child' })
    expect(childClose).toHaveFocus()
    await user.keyboard('{Shift>}{Tab}{/Shift}')
    expect(screen.getByRole('link', { name: 'Child proof' })).toHaveFocus()

    await user.keyboard('{Escape}')
    expect(screen.queryByRole('dialog', { name: 'Child' })).not.toBeInTheDocument()
    expect(screen.getByRole('dialog', { name: 'Parent' })).toBeInTheDocument()
    await waitFor(() => expect(childOpener).toHaveFocus())

    await user.keyboard('{Escape}')
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument()
    await waitFor(() => expect(rootOpener).toHaveFocus())
  })

  it('consumes modal Escape before an arena shortcut can observe it', async () => {
    const arenaEscape = vi.fn()
    window.addEventListener('keydown', arenaEscape)
    const user = userEvent.setup()
    render(<NestedDialogs />)
    await user.click(screen.getByRole('button', { name: 'Open parent' }))

    await user.keyboard('{Escape}')

    expect(arenaEscape).not.toHaveBeenCalled()
    window.removeEventListener('keydown', arenaEscape)
  })
})
