import { readFileSync } from 'node:fs'
import { join } from 'node:path'

import { describe, expect, it } from 'vitest'

function rgb(hex: string): [number, number, number] {
  const value = hex.replace('#', '')
  return [0, 2, 4].map((offset) => Number.parseInt(value.slice(offset, offset + 2), 16)) as [
    number,
    number,
    number,
  ]
}

function luminance(hex: string): number {
  const channels = rgb(hex).map((channel) => {
    const value = channel / 255
    return value <= 0.04045 ? value / 12.92 : ((value + 0.055) / 1.055) ** 2.4
  })
  return 0.2126 * channels[0] + 0.7152 * channels[1] + 0.0722 * channels[2]
}

function contrast(foreground: string, background: string): number {
  const lighter = Math.max(luminance(foreground), luminance(background))
  const darker = Math.min(luminance(foreground), luminance(background))
  return (lighter + 0.05) / (darker + 0.05)
}

describe('Explain to the Room accessibility guards', () => {
  it('keeps Back text above WCAG AA contrast on its red ground', () => {
    const css = readFileSync(join(import.meta.dirname, 'styles.css'), 'utf8')
    const red = /--red:\s*(#[0-9a-f]{6})/i.exec(css)?.[1]
    const close = /\.ringside-close\s*\{([^}]*)\}/.exec(css)?.[1] ?? ''
    const foreground = /color:\s*(#[0-9a-f]{6})/i.exec(close)?.[1]
    expect(red).toBeTruthy()
    expect(foreground).toBeTruthy()
    expect(close).toMatch(/background:\s*var\(--red\)/)
    expect(contrast(foreground!, red!)).toBeGreaterThanOrEqual(4.5)
  })

  it('restores focus only while the saved opener remains connected', () => {
    const source = readFileSync(join(import.meta.dirname, 'App.tsx'), 'utf8')
    const start = source.indexOf('function RingsideTake')
    const cue = source.slice(start, source.indexOf('/** One lane', start))
    expect(cue).toMatch(/if\s*\(opener\?\.isConnected\)\s*opener\.focus\(\)/)
    expect(cue).not.toMatch(/requestAnimationFrame\(\(\)\s*=>\s*opener\?\.focus\(\)\)/)
  })
})
