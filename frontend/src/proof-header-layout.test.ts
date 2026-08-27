import { readFileSync, readdirSync, statSync } from 'node:fs'
import { join, relative } from 'node:path'

import { describe, expect, it } from 'vitest'

const sourceRoot = import.meta.dirname
const projectRoot = join(sourceRoot, '..', '..')
const css = readFileSync(join(sourceRoot, 'styles.css'), 'utf8').replace(/\/\*[\s\S]*?\*\//g, '')
const app = readFileSync(join(sourceRoot, 'App.tsx'), 'utf8')

function rule(selector: string, sheet = css): string {
  const escaped = selector.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
  return new RegExp(`${escaped}\\s*\\{([^}]*)\\}`).exec(sheet)?.[1] ?? ''
}

function blockAt(sheet: string, opener: string, from = 0): string {
  const start = sheet.indexOf(opener, from)
  if (start < 0) return ''
  let cursor = sheet.indexOf('{', start)
  let depth = 1
  const bodyStart = cursor + 1
  while (depth > 0 && cursor < sheet.length - 1) {
    cursor += 1
    if (sheet[cursor] === '{') depth += 1
    if (sheet[cursor] === '}') depth -= 1
  }
  return sheet.slice(bodyStart, cursor)
}

function markdownFiles(directory: string): string[] {
  return readdirSync(directory).flatMap((name) => {
    if (name === '.git' || name === 'node_modules' || name === '.venv' || name.startsWith('.anti-demo')) return []
    const path = join(directory, name)
    return statSync(path).isDirectory() ? markdownFiles(path) : path.endsWith('.md') ? [path] : []
  })
}

describe('proof header layout', () => {
  it('reserves shrinkable title space beside both desktop controls', () => {
    const header = rule('.proof-header')
    const title = rule('.proof-header h1')
    expect(header).toMatch(/grid-template-columns:\s*minmax\(\s*50px,\s*4vw\s*\)\s+minmax\(\s*0,\s*1fr\s*\)\s+auto\s+auto/)
    expect(header).toMatch(/gap:\s*clamp\(\s*8px,\s*\.8vw,\s*16px\s*\)/)
    expect(rule('.proof-title')).toMatch(/min-width:\s*0/)
    expect(title).toMatch(/font-size:\s*clamp\(\s*18px,\s*calc\(\s*1\.8vw\s*\+\s*3px\s*\),\s*46px\s*\)/)
    expect(title).toMatch(/white-space:\s*nowrap/)

    const proof = app.slice(app.indexOf('function Proof({'), app.indexOf('function TowelControl('))
    const headerMarkup = proof.slice(proof.indexOf('<header className="proof-header">'), proof.indexOf('</header>'))
    expect(headerMarkup).toContain('className="proof-title"')
    expect(headerMarkup).toContain('className="proof-state"')
    expect(headerMarkup).toMatch(/<SoundToggle\b[^>]*\barena\b/)
  })

  it('stacks the mobile header into contained rows without hiding controls', () => {
    const mobile = blockAt(css, '@media (max-width: 700px)', css.indexOf('.proof-header'))
    expect(rule('.proof-header', mobile)).toMatch(/grid-template-columns:\s*50px\s+minmax\(\s*0,\s*1fr\s*\)\s+auto/)
    expect(rule('.proof-header > .proof-title', mobile)).toMatch(/grid-column:\s*2\s*\/\s*4/)
    expect(rule('.proof-header > .proof-state', mobile)).toMatch(/grid-column:\s*2/)
    expect(rule('.proof-header > .proof-state', mobile)).toMatch(/overflow-wrap:\s*anywhere/)
    expect(rule('.proof-header > .sound-button-arena', mobile)).toMatch(/grid-column:\s*3/)
    expect(rule('.proof-header h1', mobile)).toMatch(/white-space:\s*normal/)
    expect(rule('.proof-header h1', mobile)).toMatch(/overflow-wrap:\s*anywhere/)
  })
})

describe('US towel spelling', () => {
  it('uses Toweled in the visible proof status while preserving the wire identifier', () => {
    const stateLabel = app.slice(app.indexOf('function stateLabel('), app.indexOf('function fairnessCopy('))
    expect(stateLabel).toContain("state === 'towelled'")
    expect(stateLabel).toContain("return 'Toweled live'")
    expect(stateLabel).not.toMatch(/Towelled|TOWELLED|Towelling|TOWELLING/)
  })

  it('keeps British spelling out of public Markdown prose', () => {
    const findings = markdownFiles(projectRoot).flatMap((path) => {
      // Compatibility identifiers such as the wire state `TOWELLED` may remain
      // in code spans; prose and user-facing labels use US spelling.
      const prose = readFileSync(path, 'utf8').replace(/`[^`\n]+`/g, '')
      const matches = prose.match(/\btowell(?:ed|ing)\b/gi) ?? []
      return matches.map((match) => `${relative(projectRoot, path)}: ${match}`)
    })
    expect(findings).toEqual([])
  })
})
