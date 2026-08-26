import type { RoundId } from './api/types'
import {
  setOriginalCreditsThemeMuted,
  setOriginalRoundThemeMuted,
  setOriginalTitleThemeMuted,
  startOriginalCreditsTheme,
  startOriginalRoundTheme,
  startOriginalTitleTheme,
  stopOriginalCreditsTheme,
  stopOriginalRoundTheme,
  stopOriginalTitleTheme,
} from './audio'
import './music.css'

interface ListeningTheme {
  id: RoundId
  round: number
  title: string
  musicTitle: string
  concept: string
}

const TITLE_ID = 'title' as const
const CREDITS_ID = 'credits' as const
type ListeningId = RoundId | typeof TITLE_ID | typeof CREDITS_ID

const TITLE_THEME = {
  id: TITLE_ID,
  musicTitle: 'Final Bell',
  concept: 'A four-note championship belt motif, synthesized glove-and-rope accents, and a restrained minor-to-major payoff.',
} as const

const CREDITS_THEME = {
  id: CREDITS_ID,
  musicTitle: 'The Long Walk Back',
  concept: 'Forty bars at 110 BPM, through-composed rather than looped. D minor walks out through B-flat and F into D major, where the title theme’s D–A–C–F hook returns as half notes — D–A–C♯–F♯ — with a descant above it, and one last bar of D minor before the end.',
} as const

const THEMES: readonly ListeningTheme[] = [
  {
    id: 'wake_idle_app',
    round: 1,
    title: 'Wake this idle app',
    musicTitle: 'Cold Start',
    concept: 'D minor at 76 BPM. The arrangement boots: bass alone, then a pad, then the complete four-note hook, then back to sleep.',
  },
  {
    id: 'make_schema_change_safely',
    round: 2,
    title: 'Make this schema change safely',
    musicTitle: 'Safe Hands',
    concept: 'G dorian at 88 BPM. A strict two-bar canon — the branch restates the source note for note while the triangle proves the source never moved.',
  },
  {
    id: 'recover_deleted_order',
    round: 3,
    title: 'Recover this deleted order',
    musicTitle: 'The Missing Row',
    concept: 'A aeolian at 78 BPM. The hook plays with its third note deleted for sixteen bars, then states whole. The sparsest cue in the suite.',
  },
  {
    id: 'put_model_score_in_app',
    round: 4,
    title: 'Move lakehouse data into live applications',
    musicTitle: 'Handoff',
    concept: 'F major at 92 BPM. One line handed from the thin pulse to the warm one mid-phrase, descending as it crosses: the register motion is the direction of travel.',
  },
  {
    id: 'survive_connection_spike',
    round: 5,
    title: 'Get spike-ready',
    musicTitle: 'Hold the Line',
    concept: 'D minor over a dominant A pedal at 84 BPM. The spike is a swell, not an acceleration — four decibels louder, identical note count.',
  },
  {
    id: 'analyze_live_orders_without_slowing_checkout',
    round: 6,
    title: 'Move live application data into the lakehouse',
    musicTitle: 'Last Bell',
    concept: 'D major at 80 BPM. The parallel major of the title theme, stating the hook as D A C# F#, with a coda thin enough to hold the summary page.',
  },
]

function requiredElement<T extends Element>(selector: string): T {
  const element = document.querySelector<T>(selector)
  if (!element) throw new Error(`Listening room control is missing: ${selector}`)
  return element
}

const grid = requiredElement<HTMLElement>('#theme-grid')
const featured = requiredElement<HTMLElement>('#featured-theme')
const status = requiredElement<HTMLElement>('#playback-status')
const muteButton = requiredElement<HTMLButtonElement>('#master-mute')
const stopButton = requiredElement<HTMLButtonElement>('#master-stop')

let activeId: ListeningId | null = null
let muted = false

function themeFor(id: RoundId): ListeningTheme {
  return THEMES.find((theme) => theme.id === id) ?? THEMES[0]
}

function renderState(message?: string): void {
  for (const card of document.querySelectorAll<HTMLElement>('[data-theme-id]')) {
    const isActive = card.dataset.themeId === activeId
    card.classList.toggle('is-playing', isActive)
    const button = card.querySelector<HTMLButtonElement>('button')
    if (button) {
      button.textContent = isActive ? 'Stop' : 'Play'
      button.setAttribute('aria-pressed', String(isActive))
      button.setAttribute('aria-label', `${isActive ? 'Stop' : 'Play'} ${card.dataset.musicTitle}`)
    }
  }

  stopButton.disabled = activeId === null
  muteButton.textContent = muted ? 'Unmute' : 'Mute'
  muteButton.setAttribute('aria-pressed', String(muted))
  document.body.classList.toggle('is-active', activeId !== null)

  if (message) {
    status.textContent = message
  } else if (activeId === TITLE_ID) {
    status.textContent = `${muted ? 'Playing muted' : 'Playing'} — Featured title theme: ${TITLE_THEME.musicTitle}`
  } else if (activeId === CREDITS_ID) {
    status.textContent = `${muted ? 'Playing muted' : 'Playing'} — End titles: ${CREDITS_THEME.musicTitle}`
  } else if (activeId) {
    const theme = themeFor(activeId)
    status.textContent = `${muted ? 'Playing muted' : 'Playing'} — Round ${theme.round}: ${theme.musicTitle}`
  } else {
    status.textContent = muted ? 'Ready — master audio muted' : 'Ready — choose a theme'
  }
}

function stopAll(): void {
  stopOriginalRoundTheme()
  stopOriginalTitleTheme()
  stopOriginalCreditsTheme()
  activeId = null
  renderState()
}

function startTheme(id: ListeningId): boolean {
  if (id === TITLE_ID) return startOriginalTitleTheme()
  if (id === CREDITS_ID) return startOriginalCreditsTheme()
  return startOriginalRoundTheme(id)
}

function toggleTheme(id: ListeningId): void {
  if (activeId === id) {
    stopAll()
    return
  }

  if (activeId) {
    stopOriginalRoundTheme()
    stopOriginalTitleTheme()
    stopOriginalCreditsTheme()
  }
  const started = startTheme(id)
  activeId = started ? id : null
  renderState(activeId ? undefined : 'Audio is unavailable in this browser')
}

featured.innerHTML = `
  <article class="theme-card featured-card" data-theme-id="${TITLE_THEME.id}" data-music-title="${TITLE_THEME.musicTitle}">
    <div class="round-number" aria-hidden="true">★</div>
    <div class="theme-copy">
      <p class="round-label">Featured · Attract mode</p>
      <h2>Start Screen</h2>
      <h3>${TITLE_THEME.musicTitle}</h3>
      <p>${TITLE_THEME.concept}</p>
    </div>
    <button class="play-button" type="button" aria-pressed="false" aria-label="Play ${TITLE_THEME.musicTitle}">Play</button>
  </article>
  <article class="theme-card featured-card" data-theme-id="${CREDITS_THEME.id}" data-music-title="${CREDITS_THEME.musicTitle}">
    <div class="round-number" aria-hidden="true">☆</div>
    <div class="theme-copy">
      <p class="round-label">Featured · End titles</p>
      <h2>Credits Roll</h2>
      <h3>${CREDITS_THEME.musicTitle}</h3>
      <p>${CREDITS_THEME.concept}</p>
    </div>
    <button class="play-button" type="button" aria-pressed="false" aria-label="Play ${CREDITS_THEME.musicTitle}">Play</button>
  </article>
`

grid.innerHTML = THEMES.map((theme) => `
  <article class="theme-card" data-theme-id="${theme.id}" data-music-title="${theme.musicTitle}">
    <div class="round-number" aria-hidden="true">${String(theme.round).padStart(2, '0')}</div>
    <div class="theme-copy">
      <p class="round-label">Round ${theme.round}</p>
      <h2>${theme.title}</h2>
      <h3>${theme.musicTitle}</h3>
      <p>${theme.concept}</p>
    </div>
    <button class="play-button" type="button" aria-pressed="false" aria-label="Play ${theme.musicTitle}">Play</button>
  </article>
`).join('')

document.addEventListener('click', (event) => {
  const button = (event.target as Element).closest<HTMLButtonElement>('.play-button')
  const card = button?.closest<HTMLElement>('[data-theme-id]')
  if (card?.dataset.themeId) toggleTheme(card.dataset.themeId as ListeningId)
})

muteButton.addEventListener('click', () => {
  muted = !muted
  setOriginalRoundThemeMuted(muted)
  setOriginalTitleThemeMuted(muted)
  setOriginalCreditsThemeMuted(muted)
  renderState()
})

stopButton.addEventListener('click', stopAll)

document.addEventListener('keydown', (event) => {
  if (event.altKey || event.ctrlKey || event.metaKey) return
  const round = Number(event.key)
  if (round >= 1 && round <= THEMES.length) {
    event.preventDefault()
    toggleTheme(THEMES[round - 1].id)
  } else if (event.key.toLowerCase() === 't') {
    event.preventDefault()
    toggleTheme(TITLE_ID)
  } else if (event.key.toLowerCase() === 'c') {
    event.preventDefault()
    toggleTheme(CREDITS_ID)
  } else if (event.key.toLowerCase() === 'm') {
    event.preventDefault()
    muteButton.click()
  } else if (event.key === 'Escape') {
    event.preventDefault()
    stopAll()
  }
})

window.addEventListener('pagehide', stopAll)
renderState()
