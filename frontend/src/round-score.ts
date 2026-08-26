/**
 * THE ROUND SUITE — six cues, one score.
 * ===========================================================================
 *
 * Six underscore cues for the six rounds, written for the 2A03: two pulse
 * channels, one triangle, one noise, four voices at the absolute ceiling.
 * Square and triangle oscillators only, no filters anywhere.
 *
 * This module is score and planner only. It builds no audio graph and touches
 * no AudioContext — `audio.ts` owns the runtime and consumes `roundCuePlan()`.
 *
 * Why it is a separate module rather than more of `audio.ts`: the score data is
 * the bulk of it and it is edited for musical reasons, while the runtime is
 * edited for audio reasons. They also have different review audiences.
 *
 * WHAT MAKES SIX TRACKS SOUND LIKE ONE HAND
 *
 * 1. One motif. The title theme's hook is degrees 1, 5, b7, b3-above —
 *    D5 A4 C5 F5. Every cue states those four degrees, augmented to
 *    8/4/4/16 sixteenths so they take six seconds instead of one and a half.
 *    Its major form, 1, 5, 7, 3-above, is the D5 A4 C#5 F#5 the credits cue
 *    quotes; Round 6 states exactly that an octave down. The soundtrack is
 *    bookended by the same four notes.
 * 2. Within a cue the motif never transposes. The chords move underneath and
 *    recolour it: tonic over i, fifth over IV, ninth over bVI, eleventh over
 *    bVII. Four notes, four meanings, no melodic sequence — which is why the
 *    hook survives ninety seconds without nagging.
 * 3. One chord cycle: i - IV - bVI - bVII, two bars each, the IV borrowed from
 *    dorian so it lands major. Rounds 4 and 6 run the major form, I - IV - vi - V.
 * 4. One countersubject: the lament tetrachord 1-b7-b6-5 in the triangle,
 *    always in the third eight, where each cue opens up.
 * 5. One key area — every cue is a mode or relative of the title theme's D
 *    minor: D (R1), G dorian (R2), A aeolian (R3), F major (R4), D minor on
 *    its dominant (R5), D major (R6, the ending).
 *
 * UNDERSCORE DISCIPLINE (this music plays while a human is talking to a room)
 *   - 76..92 BPM, no double time, no sixteenth-note figuration anywhere
 *   - the two pulse channels are intermittent, not continuous
 *   - noise periods are floored at 508, so nothing hisses over consonants
 *   - bar 1 of every intro is empty above 500 Hz, so `playOriginalBell`
 *     (620 / 930 / 1240 Hz) rings clear through the top of the cue
 *   - every cue thins to near-nothing somewhere in its loop, which is where a
 *     presenter gets a quiet window without touching a fader
 */

import type { RoundId } from './api/types'

/* -------------------------------------------------------------------------- *
 * Types
 * -------------------------------------------------------------------------- */

export type RoundPart = 'intro' | 'loop'
export type RoundChannel = 'pulse1' | 'pulse2' | 'triangle' | 'noise'

/** Sixteen slots per bar: a pitch in Hz, `CUT` for a release, or null to hold. */
export type BarSteps = readonly (number | null)[]
export type DrumSteps = readonly (string | null)[]

/** `[bar, level]` control points, smoothstepped between. */
export type DynamicCurve = readonly (readonly [number, number])[]
/** `[fraction of the note, duty]` — a duty rewrite partway through a note. */
export type DutyEnvelope = readonly (readonly [number, number])[]

export interface Vibrato {
  readonly rate: number
  readonly cents: number
  /** Fraction of the note to wait before the vibrato starts. */
  readonly delay: number
}

/** A per-note override, keyed `part:bar:step` (or `part:p2:bar:step`). */
export interface RoundHold {
  readonly steps: number
  readonly vib?: Vibrato
  readonly duty?: DutyEnvelope
  readonly from?: number
  readonly attack?: number
  readonly gain?: number
}

export interface RoundVoice {
  readonly duty?: number
  readonly maxGate: number
  readonly attack: number
  readonly sustain: number
  readonly volume: number
}

export interface RoundBlock {
  readonly id: string
  readonly part: RoundPart
  readonly startBar: number
  readonly lead: RoundVoice
  readonly p2: RoundVoice
  readonly tri: RoundVoice
}

export type Pulse2Kind = 'silent' | 'arp' | 'echo' | 'melody' | 'canon'

export interface Pulse2Spec {
  readonly kind: Pulse2Kind
  /** echo: delay in steps. */
  readonly steps?: number
  /** canon: delay in bars. */
  readonly bars?: number
  readonly semitones?: number
  readonly duty?: number
  readonly attack?: number
  readonly gain?: number
  readonly maxGate?: number
}

export type Pulse2Mode = Pulse2Kind | Pulse2Spec

export type ArpChord = readonly (string | number)[]

export interface RoundScorePart {
  readonly pulse1?: BarSteps
  readonly pulse2?: BarSteps
  readonly p2mode?: readonly Pulse2Mode[]
  /** Bar index -> the chords that bar cycles through. */
  readonly arp?: Readonly<Record<number, readonly (ArpChord | null)[]>>
  readonly triangle?: BarSteps
  readonly noise?: DrumSteps
}

export interface RoundLandmark {
  readonly id: string
  readonly label: string
  readonly part: RoundPart
  readonly bar: number
}

export interface RoundCue {
  readonly id: string
  readonly round: number
  readonly roundId: RoundId
  readonly title: string
  readonly musicTitle: string
  readonly key: string
  readonly bpm: number
  readonly master: number
  readonly introBars: number
  readonly loopBars: number
  readonly drumScale?: number
  readonly concept: string
  readonly dramatic: string
  readonly safety: string
  readonly landmarks: readonly RoundLandmark[]
  readonly blocks: readonly RoundBlock[]
  readonly holds?: Readonly<Record<string, RoundHold>>
  readonly dynamics: Readonly<Record<RoundPart, DynamicCurve>>
  readonly echoSteps?: number
  readonly accents?: readonly number[]
  readonly score: Readonly<Record<RoundPart, RoundScorePart>>
}

export interface RoundEvent {
  readonly channel: RoundChannel
  readonly kind: 'note' | 'arp' | 'noise'
  readonly step: number
  readonly time: number
  readonly duration: number
  readonly gain: number
  readonly block: string
  readonly frequency?: number
  readonly chord?: readonly number[]
  readonly duty?: number
  readonly dutyEnvelope?: DutyEnvelope | null
  readonly vibrato?: Vibrato | null
  readonly portamentoFrom?: number | null
  readonly attack?: number
  readonly sustain?: number
  readonly period?: number
  readonly rateFrom?: number
  readonly token?: string
}

export interface RoundSection {
  readonly id: string
  readonly label: string
  readonly part: RoundPart
  readonly startBar: number
  readonly startSeconds: number
}

export interface RoundPlan {
  readonly id: string
  readonly bpm: number
  readonly stepSeconds: number
  readonly barSeconds: number
  readonly introDuration: number
  readonly loopDuration: number
  readonly introEvents: readonly RoundEvent[]
  readonly loopEvents: readonly RoundEvent[]
  readonly sections: readonly RoundSection[]
}

/* -------------------------------------------------------------------------- *
 * Grid
 * -------------------------------------------------------------------------- */

const STEPS_PER_BAR = 16
/** Silence between two notes on one channel. Reads as a hardware gate. */
const VOICE_GAP = 0.006

/* -------------------------------------------------------------------------- *
 * Pitch
 * -------------------------------------------------------------------------- */

const NAMES = ['C', 'Cs', 'D', 'Eb', 'E', 'F', 'Fs', 'G', 'Ab', 'A', 'Bb', 'B']
const ALIASES: Readonly<Record<string, string>> = {
  Db: 'Cs', 'D#': 'Eb', Ds: 'Eb', 'F#': 'Fs', Gb: 'Fs',
  'G#': 'Ab', Gs: 'Ab', 'A#': 'Bb', As: 'Bb', 'C#': 'Cs',
}

/** MIDI number for a name like `Bb3`. */
export function midi(name: string): number {
  const match = /^([A-Ga-g][b#s]?)(-?\d)$/.exec(name)
  if (!match) throw new Error(`Not a pitch: ${name}`)
  const raw = match[1][0].toUpperCase() + match[1].slice(1)
  const canonical = NAMES.includes(raw) ? raw : ALIASES[raw]
  if (!canonical) throw new Error(`Not a pitch: ${name}`)
  return (Number(match[2]) + 1) * 12 + NAMES.indexOf(canonical)
}

/** Equal-tempered frequency of a MIDI number. A4 = 440. */
export const hz = (note: number): number => 440 * 2 ** ((note - 69) / 12)

const pitchCache = new Map<string, number>()

/** Frequency of a pitch name, memoised. */
export function P(name: string): number {
  const cached = pitchCache.get(name)
  if (cached !== undefined) return cached
  const value = hz(midi(name))
  pitchCache.set(name, value)
  return value
}

/* -------------------------------------------------------------------------- *
 * Mini-notation
 * -------------------------------------------------------------------------- *
 * A bar is sixteen tokens. `|` is decoration and is stripped, `.` holds
 * whatever is sounding, `x` cuts it. Sustained underscore lines are written as
 * an onset and an explicit release rather than as a gate length in steps: the
 * difference between reading "D4 for a bar and a half" and counting
 * sixteenths. Writing 32 bars of four channels as arrays of 512 numbers is how
 * you end up with a transcription error you cannot hear until bar 27.
 */

/** A release token. Distinguishable from a pitch because no pitch is 0 Hz. */
export const CUT = 0

export function bar(text: string): BarSteps {
  const tokens = text.split('|').join(' ').trim().split(/\s+/)
  if (tokens.length !== STEPS_PER_BAR) {
    throw new Error(`Bar needs ${STEPS_PER_BAR} tokens, got ${tokens.length}: ${text}`)
  }
  return tokens.map((token) => {
    if (token === '.' || token === '-') return null
    if (token === 'x') return CUT
    return P(token)
  })
}

/** One bar of drum tokens; kept as strings for the DRUMS table to resolve. */
export function drums(text: string): DrumSteps {
  const tokens = text.split('|').join(' ').trim().split(/\s+/)
  if (tokens.length !== STEPS_PER_BAR) {
    throw new Error(`Drum bar needs ${STEPS_PER_BAR} tokens, got ${tokens.length}: ${text}`)
  }
  return tokens.map((token) => (token === '.' || token === '-' ? null : token))
}

/** Concatenate bars into one step array. Generic so drums share the helper. */
export const bars = <T,>(...list: readonly (readonly T[])[]): readonly T[] => list.flat()

/** `x(8, REST_BAR)` — repeat a bar n times. */
export const x = <T,>(count: number, oneBar: readonly T[]): readonly T[] =>
  Array.from({ length: count }, () => oneBar).flat()

export const REST_BAR = bar('. . . . | . . . . | . . . . | . . . .')
export const REST_DRUMS = drums('. . . . | . . . . | . . . . | . . . .')

/* -------------------------------------------------------------------------- *
 * Percussion — the dark half of the LFSR table
 * -------------------------------------------------------------------------- *
 * The 2A03 noise period sets the shift-register clock: 1789773 / period. The
 * bright end of the table (periods 32..254, clocks 7 kHz..56 kHz) is exactly
 * where speech consonants live, so the round suite does not use it at all.
 * Everything here is period >= 508, i.e. a clock at or below 3.5 kHz.
 */
interface Drum {
  readonly period: number
  readonly volume: number
  readonly decay: number
  readonly rateFrom: number
}

const DRUMS: Readonly<Record<string, Drum>> = {
  // deep booms: structural downbeats, the ring floor
  B: { period: 4068, volume: 0.030, decay: 0.34, rateFrom: 1.5 },
  b: { period: 4068, volume: 0.018, decay: 0.26, rateFrom: 1.35 },
  // toms
  T: { period: 2034, volume: 0.021, decay: 0.20, rateFrom: 1.45 },
  t: { period: 2034, volume: 0.013, decay: 0.15, rateFrom: 1.3 },
  // brushed accents; the brightest thing in the suite and still sub-2 kHz
  w: { period: 762, volume: 0.0075, decay: 0.075, rateFrom: 1 },
  W: { period: 508, volume: 0.0095, decay: 0.10, rateFrom: 1.15 },
  // a slow swell, used at section joins
  S: { period: 1016, volume: 0.011, decay: 0.42, rateFrom: 0.72 },
  // rim tick, felt more than heard
  r: { period: 762, volume: 0.0055, decay: 0.045, rateFrom: 1 },
}

/* -------------------------------------------------------------------------- *
 * Planning — pure, needs no AudioContext
 * -------------------------------------------------------------------------- */

const smoothstep = (t: number): number => t * t * (3 - 2 * t)

function interpolate(curve: DynamicCurve | undefined, atBar: number): number {
  if (!curve || !curve.length) return 1
  if (atBar <= curve[0][0]) return curve[0][1]
  for (let i = 1; i < curve.length; i += 1) {
    const [barB, levelB] = curve[i]
    if (atBar <= barB) {
      const [barA, levelA] = curve[i - 1]
      const span = barB - barA
      const t = span <= 0 ? 1 : smoothstep((atBar - barA) / span)
      return levelA + (levelB - levelA) * t
    }
  }
  return curve[curve.length - 1][1]
}

/**
 * Accent map. Underscore does not want a backbeat punching through a sentence,
 * so the whole range here is 0.94..1.08 — enough to keep a bar from sounding
 * quantised, not enough to be a groove.
 */
const DEFAULT_ACCENTS: readonly number[] = [
  1.08, 0.96, 0.98, 0.95, 1.02, 0.96, 0.99, 0.94,
  1.05, 0.96, 0.98, 0.95, 1.01, 0.97, 1.00, 0.96,
  1.06, 0.95, 0.99, 0.96, 1.02, 0.95, 0.98, 0.95,
  1.04, 0.97, 0.98, 0.94, 1.00, 0.96, 1.01, 0.98,
]

function blockFor(cue: RoundCue, part: RoundPart, barIndex: number): RoundBlock {
  let found: RoundBlock | null = null
  for (const block of cue.blocks) {
    if (block.part === part && barIndex >= block.startBar) found = block
  }
  return found ?? cue.blocks.find((block) => block.part === part) ?? cue.blocks[0]
}

/** Nothing in the suite swings: the grid is the grid. */
function stepTime(step: number, stepSeconds: number): number {
  return step * stepSeconds
}

function onsetsOf(steps: BarSteps): number[] {
  const onsets: number[] = []
  for (let step = 0; step < steps.length; step += 1) {
    if (steps[step] !== null && steps[step] !== undefined) onsets.push(step)
  }
  return onsets
}

/** Shared body for the two written pitch channels. */
function pitchEvents(
  cue: RoundCue,
  channel: 'pulse1' | 'triangle',
  steps: BarSteps,
  part: RoundPart,
  stepSeconds: number,
  partSeconds: number,
  dynamics: DynamicCurve,
  accents: readonly number[],
): RoundEvent[] {
  const onsets = onsetsOf(steps)
  const events: RoundEvent[] = []
  for (let i = 0; i < onsets.length; i += 1) {
    const step = onsets[i]
    const pitch = steps[step]
    // A `x` token is a release, not a note: it ends whatever is sounding and
    // schedules nothing of its own.
    if (pitch === CUT || pitch === null) continue
    const barIndex = Math.floor(step / STEPS_PER_BAR)
    const container = blockFor(cue, part, barIndex)
    const block = channel === 'triangle' ? container.tri : container.lead
    const at = stepTime(step, stepSeconds)
    const next = onsets[i + 1]
    const last = next === undefined
    const until = (last ? partSeconds : stepTime(next, stepSeconds)) - at
    const hold = cue.holds?.[`${part}:${barIndex}:${step % STEPS_PER_BAR}`]
    const wanted = (hold ? hold.steps : block.maxGate) * stepSeconds
    const shaped = interpolate(dynamics, barIndex + (step % STEPS_PER_BAR) / STEPS_PER_BAR)
    events.push({
      channel,
      kind: 'note',
      step,
      time: at,
      duration: last
        ? Math.max(0.02, Math.min(wanted, partSeconds - at))
        : Math.max(0.02, Math.min(wanted, until - VOICE_GAP)),
      frequency: pitch,
      duty: block.duty ?? 0.5,
      dutyEnvelope: hold?.duty ?? null,
      vibrato: hold?.vib ?? null,
      portamentoFrom: hold?.from ?? null,
      attack: hold?.attack ?? block.attack,
      sustain: hold ? Math.max(block.sustain, 0.8) : block.sustain,
      // The bass is the floor of the mix; it follows the dynamics only partly,
      // or a thinned-out bar loses its footing entirely.
      gain: block.volume
        * (channel === 'triangle' ? 1 : accents[step % accents.length])
        * (channel === 'triangle' ? 0.82 + 0.18 * shaped : shaped)
        * (hold?.gain ?? 1),
      block: container.id,
    })
  }
  return events
}

/** A pulse-2 event before the channel's monophony has been resolved. */
interface Pulse2Draft {
  readonly event: RoundEvent | null
  readonly time: number
  readonly wanted: number
}

/**
 * pulse2 is the second voice and it changes job by the bar:
 *   silent  — the most important mode in an underscore
 *   arp     — arpeggio-as-chord: one channel, a triad, fused by the ear
 *   echo    — a quieter, delayed copy of pulse1: a delay line the NES lacked
 *   melody  — its own written line
 *   canon   — pulse1's line again, N bars later, optionally transposed
 */
function pulse2Events(
  cue: RoundCue,
  score: RoundScorePart,
  part: RoundPart,
  stepSeconds: number,
  partSeconds: number,
  dynamics: DynamicCurve,
  accents: readonly number[],
): RoundEvent[] {
  const drafts: Pulse2Draft[] = []
  const modes = score.p2mode ?? []
  const written = score.pulse2 ?? []
  const lead = score.pulse1 ?? []

  for (let barIndex = 0; barIndex < modes.length; barIndex += 1) {
    const rawMode = modes[barIndex]
    if (!rawMode || rawMode === 'silent') continue
    const mode: Pulse2Spec = typeof rawMode === 'string' ? { kind: rawMode } : rawMode
    const container = blockFor(cue, part, barIndex)
    const block = container.p2
    const base = barIndex * STEPS_PER_BAR

    if (mode.kind === 'arp') {
      const chords = score.arp?.[barIndex]
      if (!chords) continue
      const slots = chords.length
      const span = STEPS_PER_BAR / slots
      for (let slot = 0; slot < slots; slot += 1) {
        const chord = chords[slot]
        if (!chord) continue
        const step = base + slot * span
        const pitches = chord.map((name) => (typeof name === 'string' ? P(name) : name))
        drafts.push({
          time: stepTime(step, stepSeconds),
          wanted: span * stepSeconds,
          event: {
            channel: 'pulse2',
            kind: 'arp',
            step,
            time: stepTime(step, stepSeconds),
            duration: 0,
            chord: pitches,
            frequency: pitches[0],
            duty: mode.duty ?? block.duty,
            attack: mode.attack ?? 0.02,
            sustain: 0.92,
            gain: block.volume * (mode.gain ?? 1) * interpolate(dynamics, barIndex + slot / slots),
            block: container.id,
          },
        })
      }
      continue
    }

    const source = mode.kind === 'echo' || mode.kind === 'canon' ? lead : written
    const shift = mode.kind === 'echo'
      ? (mode.steps ?? cue.echoSteps ?? 2)
      : mode.kind === 'canon' ? (mode.bars ?? 2) * STEPS_PER_BAR : 0
    const semis = mode.semitones ?? 0
    for (let local = 0; local < STEPS_PER_BAR; local += 1) {
      const from = base + local - shift
      if (from < 0 || from >= source.length) continue
      const pitch = source[from]
      if (pitch === null || pitch === undefined) continue
      const at = stepTime(base + local, stepSeconds)
      // A cut in the source is a cut in the copy: it truncates whatever the
      // channel is holding without sounding anything itself.
      if (pitch === CUT) {
        drafts.push({ time: at, wanted: 0, event: null })
        continue
      }
      const hold = cue.holds?.[`${part}:p2:${barIndex}:${local}`]
      drafts.push({
        time: at,
        wanted: (hold ? hold.steps : (mode.maxGate ?? block.maxGate)) * stepSeconds,
        event: {
          channel: 'pulse2',
          kind: 'note',
          step: base + local,
          time: at,
          duration: 0,
          frequency: semis ? pitch * 2 ** (semis / 12) : pitch,
          duty: mode.duty ?? block.duty,
          dutyEnvelope: hold?.duty ?? null,
          vibrato: hold?.vib ?? null,
          portamentoFrom: hold?.from ?? null,
          attack: block.attack,
          sustain: hold ? Math.max(block.sustain, 0.8) : block.sustain,
          gain: block.volume
            * accents[(base + local) % accents.length]
            * interpolate(dynamics, barIndex + local / STEPS_PER_BAR)
            * (mode.gain ?? (mode.kind === 'echo' ? 0.6 : 1)),
          block: container.id,
        },
      })
    }
  }

  drafts.sort((a, b) => a.time - b.time)

  // Monophony for pulse2 is resolved after the merge, because the channel swaps
  // job mid-bar and a per-mode truncation would let an arpeggio outlive the
  // echo that is supposed to interrupt it.
  const events: RoundEvent[] = []
  for (let i = 0; i < drafts.length; i += 1) {
    const draft = drafts[i]
    if (!draft.event) continue
    const next = drafts[i + 1]
    const last = !next
    const until = (last ? partSeconds : next.time) - draft.time
    const duration = last
      ? Math.max(0.02, Math.min(draft.wanted, partSeconds - draft.time))
      : Math.max(0.02, Math.min(draft.wanted, until - VOICE_GAP))
    if (duration > 0.004) events.push({ ...draft.event, duration })
  }
  return events
}

function noiseEvents(
  cue: RoundCue,
  steps: DrumSteps,
  part: RoundPart,
  stepSeconds: number,
  partSeconds: number,
  dynamics: DynamicCurve,
): RoundEvent[] {
  const onsets: number[] = []
  for (let step = 0; step < steps.length; step += 1) if (steps[step]) onsets.push(step)
  const scale = cue.drumScale ?? 1
  const events: RoundEvent[] = []
  for (let i = 0; i < onsets.length; i += 1) {
    const step = onsets[i]
    const token = steps[step] as string
    const drum = DRUMS[token]
    if (!drum) throw new Error(`Unknown drum token "${token}" in ${cue.id}`)
    const barIndex = Math.floor(step / STEPS_PER_BAR)
    const at = stepTime(step, stepSeconds)
    const next = onsets[i + 1]
    const last = next === undefined
    const until = (last ? partSeconds : stepTime(next, stepSeconds)) - at
    events.push({
      channel: 'noise',
      kind: 'noise',
      step,
      time: at,
      duration: last
        ? Math.max(0.015, Math.min(drum.decay, partSeconds - at))
        : Math.max(0.015, Math.min(drum.decay, until - VOICE_GAP)),
      token,
      period: drum.period,
      rateFrom: drum.rateFrom,
      gain: drum.volume * scale
        * Math.min(1.1, interpolate(dynamics, barIndex + (step % STEPS_PER_BAR) / STEPS_PER_BAR)),
      block: blockFor(cue, part, barIndex).id,
    })
  }
  return events
}

function partEvents(
  cue: RoundCue,
  part: RoundPart,
  stepSeconds: number,
  partSeconds: number,
  dynamics: DynamicCurve,
  accents: readonly number[],
): RoundEvent[] {
  const score = cue.score[part]
  return [
    ...pitchEvents(cue, 'pulse1', score.pulse1 ?? [], part, stepSeconds, partSeconds, dynamics, accents),
    ...pulse2Events(cue, score, part, stepSeconds, partSeconds, dynamics, accents),
    ...pitchEvents(cue, 'triangle', score.triangle ?? [], part, stepSeconds, partSeconds, dynamics, accents),
    ...noiseEvents(cue, score.noise ?? [], part, stepSeconds, partSeconds, dynamics),
  ].sort((a, b) => a.time - b.time || a.channel.localeCompare(b.channel))
}

/* -------------------------------------------------------------------------- *
 * Shared voicing
 * -------------------------------------------------------------------------- *
 * Long gates plus explicit `x` releases: sustained lines are written as "start
 * here, stop there" rather than as gate lengths in steps.
 */

type VoiceOverride = Partial<RoundVoice>

const voices = (
  spec: { lead?: VoiceOverride; p2?: VoiceOverride; tri?: VoiceOverride } = {},
): { lead: RoundVoice; p2: RoundVoice; tri: RoundVoice } => ({
  lead: { duty: 0.5, maxGate: 64, attack: 0.024, sustain: 0.82, volume: 0.0165, ...spec.lead },
  p2: { duty: 0.5, maxGate: 64, attack: 0.032, sustain: 0.9, volume: 0.0102, ...spec.p2 },
  tri: { maxGate: 112, attack: 0.016, sustain: 0.92, volume: 0.0325, ...spec.tri },
})

/**
 * Level for the whole suite. One number, so the six cues are consistent.
 *
 * The title theme sits at 0.18 because it owns the room. Round music sits
 * under narration, so it gets roughly 5 dB less rather than the 7 dB MORE that
 * the previous 0.42 gave it. Measured: this suite renders 5.5-15.6 dB quieter
 * than the themes it replaces, and 13-23 dB quieter in the 1-4 kHz band where
 * consonants live.
 */
export const ROUND_THEME_VOLUME = 0.1

const R = REST_BAR
const RD = REST_DRUMS

/* ========================================================================== *
 * ROUND 1 — "Cold Start"
 * wake_idle_app · D minor with a dorian IV · 76 BPM
 * --------------------------------------------------------------------------
 * DRAMATIC READ: the app is at scale zero. Nothing is running. A connection
 * arrives and the system assembles itself, part by part.
 *
 * The motif arrives in pieces and the arrangement wakes up with it: two notes
 * over a bare bass for the first eight, three notes and a pad for the second,
 * the complete four-note statement in the third, then the last eight powers
 * back down so the loop restarts from stillness instead of from a wall.
 * ========================================================================== */

const R1: RoundCue = {
  id: 'r1',
  round: 1,
  roundId: 'wake_idle_app',
  title: 'Wake this idle app',
  musicTitle: 'Cold Start',
  key: 'D minor (dorian IV)',
  bpm: 76,
  master: ROUND_THEME_VOLUME,
  introBars: 4,
  loopBars: 32,
  drumScale: 1,
  concept: 'Scale-zero stillness that assembles itself: the hook arrives two notes at a time, then powers back down.',
  dramatic: 'Nothing is running. A connection lands and the system comes up part by part — bass first, pad second, the full hook third, then back to sleep.',
  safety: 'The first eight bars are bass and two soft booms only, so the opening twenty-five seconds are effectively a rest for the presenter. Nothing above 500 Hz until bar 11 of the loop.',
  landmarks: [
    { id: 'intro', label: 'Intro — one low D, one deep boom', part: 'intro', bar: 0 },
    { id: 'a', label: 'A — asleep: bass alone, hook in fragments', part: 'loop', bar: 0 },
    { id: 'b', label: "A' — the pad comes online, three notes of the hook", part: 'loop', bar: 8 },
    { id: 'c', label: 'B — awake: the complete hook, and its echo', part: 'loop', bar: 16 },
    { id: 'd', label: 'C — powering down into the seam', part: 'loop', bar: 24 },
  ],
  blocks: [
    { id: 'intro', part: 'intro', startBar: 0, ...voices({ lead: { volume: 0.0145 }, p2: { volume: 0.0088 }, tri: { volume: 0.030 } }) },
    { id: 'a', part: 'loop', startBar: 0, ...voices({ lead: { volume: 0.0155 }, p2: { volume: 0.0092 } }) },
    { id: 'b', part: 'loop', startBar: 8, ...voices({ lead: { volume: 0.0162 }, p2: { volume: 0.0100 } }) },
    { id: 'c', part: 'loop', startBar: 16, ...voices({ lead: { duty: 0.25, volume: 0.0172 }, p2: { volume: 0.0108 }, tri: { volume: 0.0335 } }) },
    { id: 'd', part: 'loop', startBar: 24, ...voices({ lead: { volume: 0.0145 }, p2: { volume: 0.0086 }, tri: { volume: 0.0305 } }) },
  ],
  holds: {
    // The arrival of the complete hook: a bar-long F4 with a slow, shallow
    // vibrato. 3.4 Hz at 14 cents is a singer leaning on a long note, not a
    // chiptune warble.
    'loop:19:0': { steps: 18, vib: { rate: 3.4, cents: 14, delay: 0.45 } },
  },
  dynamics: {
    intro: [[0, 0.52], [3, 0.78], [4, 0.8]],
    loop: [[0, 0.62], [8, 0.74], [16, 0.96], [21, 1.0], [24, 0.8], [30, 0.62], [32, 0.62]],
  },
  echoSteps: 2,
  score: {
    intro: {
      pulse1: bars(
        R,
        R,
        bar('. . . . | . . . . | D4 . . . | . . . .'),
        bar('. . . . | . . . . | A3 . . . | . . . .'),
      ),
      pulse2: [],
      p2mode: ['silent', 'silent', 'silent', 'arp'],
      arp: { 3: [['D3', 'F3', 'A3']] },
      triangle: bars(
        bar('D3 . . . | . . . . | . . . . | . . . .'),
        R,
        bar('A2 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | D3 . . . | . . . .'),
      ),
      noise: bars(
        drums('B . . . | . . . . | . . . . | . . . .'),
        RD,
        RD,
        drums('. . . . | . . . . | . . . . | S . . .'),
      ),
    },
    loop: {
      // Dm | G | Bb | C, two bars each, four times through.
      pulse1: bars(
        // A — the hook, first two notes only, twice.
        R,
        R,
        bar('D4 . . . | . . . . | A3 . . . | . . . .'),
        bar('. . . . | x . . . | . . . . | . . . .'),
        R,
        R,
        bar('. . . . | . . . . | D4 . . . | A3 . . .'),
        bar('. . . . | x . . . | . . . . | . . . .'),
        // A' — three notes now: the b7 arrives.
        R,
        R,
        bar('D4 . . . | . . . . | A3 . . . | C4 . . .'),
        bar('. . . . | . . . . | x . . . | . . . .'),
        R,
        R,
        bar('. . . . | . . . . | A3 . . . | C4 . . .'),
        bar('. . . . | x . . . | . . . . | . . . .'),
        // B — awake. Pickup, complete statement, bar-long arrival, answer.
        R,
        bar('. . . . | . . . . | . . . . | A3 . . .'),
        bar('D4 . . . | . . . . | A3 . . . | C4 . . .'),
        bar('F4 . . . | . . . . | . . . . | . . . .'),
        bar('. . x . | . . . . | . . . . | . . . .'),
        R,
        bar('. . . . | . . . . | E4 . . . | . . . .'),
        bar('D4 . . . | . . . . | . . . . | x . . .'),
        // C — powering down.
        R,
        R,
        bar('D4 . . . | . . . . | A3 . . . | . . . .'),
        bar('. . . . | x . . . | . . . . | . . . .'),
        R,
        R,
        R,
        R,
      ),
      pulse2: [],
      p2mode: [
        'silent', 'silent', 'silent', 'silent', 'silent', 'silent', 'silent', 'silent',
        'arp', 'arp', 'silent', 'silent', 'arp', 'arp', 'silent', 'silent',
        'arp', 'arp', { kind: 'echo', gain: 0.5, maxGate: 10 }, { kind: 'echo', gain: 0.5, maxGate: 10 }, { kind: 'echo', gain: 0.5, maxGate: 10 },
        'arp', { kind: 'echo', gain: 0.46, maxGate: 10 }, { kind: 'echo', gain: 0.46, maxGate: 10 },
        'arp', 'arp', 'silent', 'silent', 'silent', 'silent', 'silent', 'silent',
      ],
      arp: {
        8: [['D3', 'F3', 'A3']],
        9: [['D3', 'F3', 'A3']],
        12: [['Bb2', 'D3', 'F3']],
        13: [['Bb2', 'D3', 'F3']],
        16: [['D3', 'F3', 'A3']],
        17: [['D3', 'F3', 'A3']],
        21: [['Bb2', 'D3', 'F3']],
        24: [['D3', 'F3', 'A3']],
        25: [['D3', 'F3', 'A3']],
      },
      triangle: bars(
        bar('D3 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | A2 . . . | . . . .'),
        bar('G2 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | D3 . . . | . . . .'),
        bar('Bb2 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | F2 . . . | . . . .'),
        bar('C3 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | A2 . . . | . . . .'),
        bar('D3 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | C3 . . . | . . . .'),
        bar('G2 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | A2 . . . | . . . .'),
        bar('Bb2 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | D3 . . . | . . . .'),
        bar('C3 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | E3 . . . | . . . .'),
        // The lament tetrachord, D-C-Bb-A, in half notes across two bars.
        bar('D3 . . . | . . . . | C3 . . . | . . . .'),
        bar('Bb2 . . . | . . . . | A2 . . . | . . . .'),
        bar('G2 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | D3 . . . | . . . .'),
        bar('Bb2 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | F2 . . . | . . . .'),
        bar('C3 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | A2 . . . | . . . .'),
        bar('D3 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | A2 . . . | . . . .'),
        bar('G2 . . . | . . . . | . . . . | . . . .'),
        R,
        bar('Bb2 . . . | . . . . | . . . . | . . . .'),
        R,
        bar('C3 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | A2 . . . | . . . .'),
      ),
      noise: bars(
        drums('B . . . | . . . . | . . . . | . . . .'),
        RD, RD, RD,
        drums('b . . . | . . . . | . . . . | . . . .'),
        RD, RD, RD,
        drums('B . . . | . . . . | . . . . | . . . .'),
        RD, RD,
        drums('. . . . | . . . . | . . . . | w . . .'),
        drums('b . . . | . . . . | . . . . | . . . .'),
        RD, RD,
        drums('. . . . | . . . . | . . . . | S . . .'),
        drums('B . . . | . . . . | . . . . | . . . .'),
        drums('. . . . | . . . . | T . . . | . . . .'),
        RD,
        drums('. . . . | . . . . | . . . . | w . . .'),
        drums('b . . . | . . . . | . . . . | . . . .'),
        RD,
        drums('. . . . | . . . . | t . . . | . . . .'),
        drums('. . . . | . . . . | . . . . | w . . .'),
        drums('b . . . | . . . . | . . . . | . . . .'),
        RD, RD,
        drums('S . . . | . . . . | . . . . | . . . .'),
        RD, RD, RD, RD,
      ),
    },
  },
}

/* ========================================================================== *
 * ROUND 2 — "Safe Hands"
 * make_schema_change_safely · G dorian · 88 BPM
 * --------------------------------------------------------------------------
 * DRAMATIC READ: a branch is an exact copy, the migration lands in the copy,
 * and the source has to be provably unchanged afterwards.
 *
 * So the cue is a strict canon. Pulse 1 is the source; pulse 2 is the branch,
 * restating pulse 1's line two bars later an octave up — not a variation, the
 * same line. Under both of them the triangle re-articulates G every two beats
 * through every chord change: the source, unchanged. At bar 17 the source
 * line gains one new note (the migration) and the branch repeats it two bars
 * later — the read-back. The last eight put source and branch in literal
 * unison: they agree.
 * ========================================================================== */

const R2: RoundCue = {
  id: 'r2',
  round: 2,
  roundId: 'make_schema_change_safely',
  title: 'Make this schema change safely',
  musicTitle: 'Safe Hands',
  key: 'G dorian',
  bpm: 88,
  master: ROUND_THEME_VOLUME,
  introBars: 4,
  loopBars: 32,
  drumScale: 0.9,
  concept: 'A strict two-bar canon: the branch restates the source line note for note while the bass proves the source never moved.',
  dramatic: 'Pulse 1 is the source, pulse 2 is the branch two bars behind it, and the triangle keeps sounding G under every chord — the source, unchanged.',
  safety: 'The canon means the two pulses take turns rather than stack; each voice on its own is four notes per two bars. Bars 7-8 of every eight are silent in both — the verification pause.',
  landmarks: [
    { id: 'intro', label: 'Intro — the source line, no copy yet', part: 'intro', bar: 0 },
    { id: 'a', label: 'A — source, then the branch answers', part: 'loop', bar: 0 },
    { id: 'b', label: "A' — both running, two bars apart", part: 'loop', bar: 8 },
    { id: 'c', label: 'B — the migration: one new note, then its read-back', part: 'loop', bar: 16 },
    { id: 'd', label: 'C — unison: source and branch agree', part: 'loop', bar: 24 },
  ],
  blocks: [
    { id: 'intro', part: 'intro', startBar: 0, ...voices({ lead: { volume: 0.0148 }, tri: { volume: 0.031 } }) },
    { id: 'a', part: 'loop', startBar: 0, ...voices({ lead: { volume: 0.0158 }, p2: { duty: 0.25, volume: 0.0086 } }) },
    { id: 'b', part: 'loop', startBar: 8, ...voices({ lead: { volume: 0.0158 }, p2: { duty: 0.25, volume: 0.0090 } }) },
    { id: 'c', part: 'loop', startBar: 16, ...voices({ lead: { volume: 0.0168 }, p2: { duty: 0.25, volume: 0.0094 }, tri: { volume: 0.0335 } }) },
    { id: 'd', part: 'loop', startBar: 24, ...voices({ lead: { volume: 0.0152 }, p2: { duty: 0.5, volume: 0.0080 }, tri: { volume: 0.031 } }) },
  ],
  holds: {
    'loop:17:0': { steps: 14, vib: { rate: 3.2, cents: 11, delay: 0.5 } },
    'loop:21:0': { steps: 14, vib: { rate: 3.2, cents: 11, delay: 0.5 } },
  },
  dynamics: {
    intro: [[0, 0.6], [4, 0.82]],
    loop: [[0, 0.72], [8, 0.84], [16, 0.98], [20, 1.0], [24, 0.82], [30, 0.7], [32, 0.7]],
  },
  score: {
    intro: {
      pulse1: bars(
        R,
        bar('. . . . | . . . . | G3 . . . | D3 . . .'),
        bar('F3 . . . | . . . . | . . . . | . . . .'),
        bar('x . . . | . . . . | . . . . | . . . .'),
      ),
      pulse2: [],
      p2mode: ['silent', 'silent', 'silent', 'silent'],
      arp: {},
      triangle: bars(
        bar('G2 . . . | . . . . | . . . . | . . . .'),
        R,
        bar('G2 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | D3 . . . | . . . .'),
      ),
      noise: bars(
        drums('B . . . | . . . . | . . . . | . . . .'),
        RD,
        RD,
        drums('. . . . | . . . . | . . . . | S . . .'),
      ),
    },
    loop: {
      // Gm | C | Eb | F, two bars each. The source line is always rooted on
      // G; the chords under it do the moving.
      pulse1: bars(
        // A — statement, then two bars of rest while the branch answers.
        bar('G3 . . . | . . . . | D3 . . . | F3 . . .'),
        bar('Bb3 . . . | . . . . | . . . . | x . . .'),
        R,
        R,
        bar('G3 . . . | . . . . | D3 . . . | F3 . . .'),
        bar('Bb3 . . . | . . . . | . . . . | x . . .'),
        R,
        R,
        // A' — the source runs continuously, so the branch overlaps it.
        bar('G3 . . . | . . . . | D3 . . . | F3 . . .'),
        bar('Bb3 . . . | . . . . | A3 . . . | . . . .'),
        bar('G3 . . . | . . . . | . . . . | x . . .'),
        R,
        bar('. . . . | . . . . | D3 . . . | F3 . . .'),
        bar('G3 . . . | . . . . | . . . . | x . . .'),
        R,
        R,
        // B — the migration. The line gains a fifth note, C4, and holds it.
        bar('G3 . . . | . . . . | D3 . . . | F3 . . .'),
        bar('C4 . . . | . . . . | . . . . | . . . .'),
        bar('. . x . | . . . . | Bb3 . . . | . . . .'),
        bar('. . . . | . . . . | x . . . | . . . .'),
        bar('G3 . . . | . . . . | D3 . . . | F3 . . .'),
        bar('C4 . . . | . . . . | . . . . | . . . .'),
        bar('. . x . | . . . . | . . . . | . . . .'),
        R,
        // C — unison verification, then both voices step back.
        bar('. . . . | . . . . | G3 . . . | Bb3 . . .'),
        bar('D4 . . . | . . . . | . . . . | x . . .'),
        R,
        R,
        bar('. . . . | . . . . | G3 . . . | F3 . . .'),
        bar('D3 . . . | . . . . | . . . . | x . . .'),
        R,
        R,
      ),
      pulse2: [],
      // The branch: pulse 1's line, two bars later, an octave up. In the last
      // eight the transposition drops to zero — literal unison, the two lanes
      // reading back the same value.
      p2mode: [
        'silent', 'silent',
        { kind: 'canon', bars: 2, semitones: 12, gain: 0.8, maxGate: 6 },
        { kind: 'canon', bars: 2, semitones: 12, gain: 0.8, maxGate: 6 },
        'silent', 'silent',
        { kind: 'canon', bars: 2, semitones: 12, gain: 0.8, maxGate: 6 },
        { kind: 'canon', bars: 2, semitones: 12, gain: 0.8, maxGate: 6 },
        'silent', 'silent',
        { kind: 'canon', bars: 2, semitones: 12, gain: 0.78, maxGate: 6 },
        { kind: 'canon', bars: 2, semitones: 12, gain: 0.78, maxGate: 6 },
        { kind: 'canon', bars: 2, semitones: 12, gain: 0.78, maxGate: 6 },
        { kind: 'canon', bars: 2, semitones: 12, gain: 0.78, maxGate: 6 },
        { kind: 'canon', bars: 2, semitones: 12, gain: 0.78, maxGate: 6 },
        { kind: 'canon', bars: 2, semitones: 12, gain: 0.78, maxGate: 6 },
        'silent', 'silent',
        { kind: 'canon', bars: 2, semitones: 12, gain: 0.84, maxGate: 7 },
        { kind: 'canon', bars: 2, semitones: 12, gain: 0.84, maxGate: 7 },
        { kind: 'canon', bars: 2, semitones: 12, gain: 0.84, maxGate: 7 },
        { kind: 'canon', bars: 2, semitones: 12, gain: 0.84, maxGate: 7 },
        { kind: 'canon', bars: 2, semitones: 12, gain: 0.84, maxGate: 7 },
        { kind: 'canon', bars: 2, semitones: 12, gain: 0.84, maxGate: 7 },
        'silent', 'silent',
        { kind: 'canon', bars: 2, semitones: 0, gain: 0.88, maxGate: 8 },
        { kind: 'canon', bars: 2, semitones: 0, gain: 0.88, maxGate: 8 },
        'silent', 'silent',
        { kind: 'canon', bars: 2, semitones: 0, gain: 0.88, maxGate: 8 },
        { kind: 'canon', bars: 2, semitones: 0, gain: 0.88, maxGate: 8 },
      ],
      arp: {},
      // G, every two beats, through every chord.
      triangle: bars(
        bar('G2 . . . | . . . . | G2 . . . | . . . .'),
        bar('G2 . . . | . . . . | . . . . | . . . .'),
        bar('C3 . . . | . . . . | G2 . . . | . . . .'),
        bar('C3 . . . | . . . . | . . . . | . . . .'),
        bar('Eb2 . . . | . . . . | G2 . . . | . . . .'),
        bar('Eb2 . . . | . . . . | . . . . | . . . .'),
        bar('F2 . . . | . . . . | G2 . . . | . . . .'),
        bar('F2 . . . | . . . . | . . . . | . . . .'),
        bar('G2 . . . | . . . . | G2 . . . | . . . .'),
        bar('G2 . . . | . . . . | . . . . | . . . .'),
        bar('C3 . . . | . . . . | G2 . . . | . . . .'),
        bar('C3 . . . | . . . . | . . . . | . . . .'),
        bar('Eb2 . . . | . . . . | G2 . . . | . . . .'),
        bar('Eb2 . . . | . . . . | . . . . | . . . .'),
        bar('F2 . . . | . . . . | G2 . . . | . . . .'),
        bar('F2 . . . | . . . . | . . . . | . . . .'),
        // The lament, transposed: G-F-Eb-D.
        bar('G2 . . . | . . . . | F2 . . . | . . . .'),
        bar('Eb2 . . . | . . . . | D3 . . . | . . . .'),
        bar('C3 . . . | . . . . | G2 . . . | . . . .'),
        bar('C3 . . . | . . . . | . . . . | . . . .'),
        bar('Eb2 . . . | . . . . | G2 . . . | . . . .'),
        bar('Eb2 . . . | . . . . | . . . . | . . . .'),
        bar('F2 . . . | . . . . | G2 . . . | . . . .'),
        bar('F2 . . . | . . . . | . . . . | . . . .'),
        bar('G2 . . . | . . . . | G2 . . . | . . . .'),
        bar('G2 . . . | . . . . | . . . . | . . . .'),
        bar('C3 . . . | . . . . | G2 . . . | . . . .'),
        bar('C3 . . . | . . . . | . . . . | . . . .'),
        bar('Eb2 . . . | . . . . | G2 . . . | . . . .'),
        bar('Eb2 . . . | . . . . | . . . . | . . . .'),
        bar('F2 . . . | . . . . | G2 . . . | . . . .'),
        bar('F2 . . . | . . . . | . . . . | . . . .'),
      ),
      noise: bars(
        drums('B . . . | . . . . | . . . . | . . . .'),
        RD, RD,
        drums('. . . . | . . . . | . . . . | r . . .'),
        drums('b . . . | . . . . | . . . . | . . . .'),
        RD, RD, RD,
        drums('B . . . | . . . . | . . . . | . . . .'),
        RD,
        drums('. . . . | . . . . | t . . . | . . . .'),
        RD,
        drums('b . . . | . . . . | . . . . | . . . .'),
        RD, RD,
        drums('. . . . | . . . . | . . . . | S . . .'),
        drums('B . . . | . . . . | . . . . | . . . .'),
        RD,
        drums('. . . . | . . . . | T . . . | . . . .'),
        RD,
        drums('b . . . | . . . . | . . . . | . . . .'),
        RD,
        drums('. . . . | . . . . | t . . . | . . . .'),
        drums('. . . . | . . . . | . . . . | w . . .'),
        drums('b . . . | . . . . | . . . . | . . . .'),
        RD, RD, RD,
        drums('S . . . | . . . . | . . . . | . . . .'),
        RD, RD, RD,
      ),
    },
  },
}

/* ========================================================================== *
 * ROUND 3 — "The Missing Row"
 * recover_deleted_order · A aeolian · 78 BPM
 * --------------------------------------------------------------------------
 * DRAMATIC READ: a row is gone. The clock runs while the recovery is
 * requested, waited on, restored and read back — and the source has to stay
 * deleted afterwards.
 *
 * The conceit is a deletion you can hear. The motif is A3 E3 [G3] C4, and for
 * the first sixteen bars the third note is simply not there: a hole exactly
 * where the ear has learned to expect a pitch. At bar 17 the G3 comes back,
 * and at bar 21 the complete four-note statement lands. Bar 25 is the source
 * check: for one bar the bass drops out entirely, because what is being
 * verified there is an absence.
 *
 * This is the sparsest cue in the suite. It is the one to talk over.
 * ========================================================================== */

const R3: RoundCue = {
  id: 'r3',
  round: 3,
  roundId: 'recover_deleted_order',
  title: 'Recover this deleted order',
  musicTitle: 'The Missing Row',
  key: 'A aeolian',
  bpm: 78,
  master: ROUND_THEME_VOLUME,
  introBars: 4,
  loopBars: 32,
  drumScale: 0.85,
  concept: 'A deletion you can hear: the hook plays with its third note missing until the recovery lands, then states it whole.',
  dramatic: 'Sixteen bars with a hole in the melody where a pitch belongs, the note restored at bar 17, and one bar of deliberate bass silence for the proof that the source is still deleted.',
  safety: 'The sparsest cue in the suite: about one event every two seconds, no pulse content at all in eleven of the thirty-two bars, and eight noise hits in a hundred seconds.',
  landmarks: [
    { id: 'intro', label: 'Intro — the row is already gone', part: 'intro', bar: 0 },
    { id: 'a', label: 'A — the hook with its third note deleted', part: 'loop', bar: 0 },
    { id: 'b', label: "A' — searching: the hole is still there", part: 'loop', bar: 8 },
    { id: 'c', label: 'B — RECOVERED: the missing G returns', part: 'loop', bar: 16 },
    { id: 'd', label: 'C — the source is still deleted (bass drops out)', part: 'loop', bar: 24 },
  ],
  blocks: [
    { id: 'intro', part: 'intro', startBar: 0, ...voices({ lead: { volume: 0.0142 }, tri: { volume: 0.0295 } }) },
    { id: 'a', part: 'loop', startBar: 0, ...voices({ lead: { volume: 0.0152 }, p2: { volume: 0.0088 }, tri: { volume: 0.0315 } }) },
    { id: 'b', part: 'loop', startBar: 8, ...voices({ lead: { volume: 0.0156 }, p2: { volume: 0.0092 }, tri: { volume: 0.0315 } }) },
    { id: 'c', part: 'loop', startBar: 16, ...voices({ lead: { duty: 0.25, volume: 0.0170 }, p2: { volume: 0.0100 }, tri: { volume: 0.033 } }) },
    { id: 'd', part: 'loop', startBar: 24, ...voices({ lead: { volume: 0.0148 }, p2: { volume: 0.0084 }, tri: { volume: 0.0305 } }) },
  ],
  holds: {
    // The recovered note, leaned on: a slow bend up into it and a long tail.
    'loop:17:8': { steps: 12, from: 195, vib: { rate: 3.1, cents: 12, delay: 0.55 } },
    'loop:21:0': { steps: 20, vib: { rate: 3.0, cents: 13, delay: 0.5 } },
  },
  dynamics: {
    intro: [[0, 0.5], [4, 0.72]],
    loop: [[0, 0.6], [8, 0.68], [16, 0.94], [22, 1.0], [24, 0.74], [30, 0.58], [32, 0.58]],
  },
  echoSteps: 3,
  score: {
    intro: {
      pulse1: bars(
        R,
        R,
        bar('. . . . | . . . . | A3 . . . | . . . .'),
        bar('. . . . | . . . . | E3 . . . | . . . .'),
      ),
      pulse2: [],
      p2mode: ['silent', 'silent', 'silent', 'silent'],
      arp: {},
      triangle: bars(
        bar('A2 . . . | . . . . | . . . . | . . . .'),
        R,
        bar('. . . . | . . . . | . . . . | . . . .'),
        bar('E2 . . . | . . . . | . . . . | . . . .'),
      ),
      noise: bars(
        drums('B . . . | . . . . | . . . . | . . . .'),
        RD, RD,
        drums('. . . . | . . . . | . . . . | S . . .'),
      ),
    },
    loop: {
      // Am | D | F | G, two bars each.
      pulse1: bars(
        // A — A3, E3, then the hole, then C4 arriving unprepared.
        bar('A3 . . . | . . . . | E3 . . . | . . . .'),
        bar('. . x . | . . . . | . . . . | . . . .'),
        R,
        bar('. . . . | . . . . | C4 . . . | . . . .'),
        bar('. . . . | . . x . | . . . . | . . . .'),
        R,
        bar('A3 . . . | . . . . | E3 . . . | . . . .'),
        bar('. . x . | . . . . | . . . . | . . . .'),
        // A' — the same, lower, and the hole is wider.
        bar('A3 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | E3 . . . | . . . . | x . . .'),
        R,
        R,
        bar('. . . . | . . . . | C4 . . . | . . . .'),
        bar('. . . . | . . x . | . . . . | . . . .'),
        R,
        bar('. . . . | . . . . | A3 . . . | . . . .'),
        // B — recovered. E3 bends up into the G3 that was missing.
        bar('A3 . . . | . . . . | E3 . . . | . . . .'),
        bar('. . . . | . . . . | G3 . . . | . . . .'),
        bar('. . . . | x . . . | . . . . | . . . .'),
        R,
        bar('A3 . . . | . . . . | E3 . . . | G3 . . .'),
        bar('C4 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | x . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | B3 . . . | . . . .'),
        // C — verified. One bar with no bass under it at all.
        bar('A3 . . . | . . . . | . . . . | x . . .'),
        R,
        bar('. . . . | . . . . | E3 . . . | . . . .'),
        bar('. . . . | x . . . | . . . . | . . . .'),
        R,
        bar('. . . . | . . . . | A3 . . . | . . . .'),
        bar('. . . . | . . x . | . . . . | . . . .'),
        R,
      ),
      pulse2: [],
      p2mode: [
        'silent', 'silent', 'silent', 'silent', 'silent', 'silent', 'silent', 'silent',
        'silent', 'silent', 'arp', 'arp', 'silent', 'silent', 'silent', 'silent',
        'silent', 'silent', { kind: 'echo', gain: 0.44, maxGate: 10 }, 'arp',
        'silent', { kind: 'echo', gain: 0.44, maxGate: 10 }, { kind: 'echo', gain: 0.44, maxGate: 10 }, 'arp',
        'silent', 'silent', 'silent', 'silent', 'arp', 'silent', 'silent', 'silent',
      ],
      arp: {
        10: [['F2', 'A2', 'C3']],
        11: [['F2', 'A2', 'C3']],
        19: [['A2', 'C3', 'E3']],
        23: [['G2', 'B2', 'D3']],
        28: [['F2', 'A2', 'C3']],
      },
      triangle: bars(
        bar('A2 . . . | . . . . | . . . . | . . . .'),
        R,
        bar('D3 . . . | . . . . | . . . . | . . . .'),
        R,
        bar('F2 . . . | . . . . | . . . . | . . . .'),
        R,
        bar('G2 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | E2 . . . | . . . .'),
        bar('A2 . . . | . . . . | . . . . | . . . .'),
        R,
        bar('D3 . . . | . . . . | . . . . | A2 . . .'),
        R,
        bar('F2 . . . | . . . . | . . . . | . . . .'),
        R,
        bar('G2 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | E2 . . . | . . . .'),
        // The lament, transposed: A-G-F-E.
        bar('A2 . . . | . . . . | G2 . . . | . . . .'),
        bar('F2 . . . | . . . . | E2 . . . | . . . .'),
        bar('D3 . . . | . . . . | . . . . | . . . .'),
        R,
        bar('F2 . . . | . . . . | . . . . | . . . .'),
        R,
        bar('G2 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | E2 . . . | . . . .'),
        // Bar 25: the source check. No bass. The cue is a lead and nothing.
        bar('A2 . . . | . . . . | . . . . | x . . .'),
        R,
        bar('D3 . . . | . . . . | . . . . | . . . .'),
        R,
        bar('F2 . . . | . . . . | . . . . | . . . .'),
        R,
        bar('G2 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | E2 . . . | . . . .'),
      ),
      noise: bars(
        drums('B . . . | . . . . | . . . . | . . . .'),
        RD, RD, RD, RD, RD,
        drums('b . . . | . . . . | . . . . | . . . .'),
        RD,
        drums('B . . . | . . . . | . . . . | . . . .'),
        RD, RD, RD, RD, RD,
        drums('. . . . | . . . . | . . . . | S . . .'),
        RD,
        drums('B . . . | . . . . | . . . . | . . . .'),
        RD,
        drums('. . . . | . . . . | t . . . | . . . .'),
        RD,
        drums('b . . . | . . . . | . . . . | . . . .'),
        RD, RD,
        drums('. . . . | . . . . | . . . . | w . . .'),
        RD, RD, RD, RD,
        drums('S . . . | . . . . | . . . . | . . . .'),
        RD, RD, RD,
      ),
    },
  },
}

/* ========================================================================== *
 * ROUND 4 — "Handoff"
 * put_model_score_in_app · F major with one lydian B · 92 BPM
 * --------------------------------------------------------------------------
 * DRAMATIC READ: governed lakehouse data crossing into a live application in
 * one platform path. OLAP to OLTP, no seam.
 *
 * So the melody is physically handed between channels mid-phrase. Pulse 2 —
 * thin 12.5% duty, upper register, the analytical side — starts each phrase;
 * pulse 1 — warm 50% duty, lower, the operational side — takes it over with a
 * one-step overlap so the join is inaudible. One line, two channels, no seam.
 * The line descends as it crosses, so the register motion is the direction of
 * travel. The single B natural over the F chord is the governed brightness:
 * used twice, both times on the way in.
 * ========================================================================== */

const R4: RoundCue = {
  id: 'r4',
  round: 4,
  roundId: 'put_model_score_in_app',
  title: 'Move lakehouse data into live applications',
  musicTitle: 'Handoff',
  key: 'F major (one lydian B)',
  bpm: 92,
  master: ROUND_THEME_VOLUME,
  introBars: 4,
  loopBars: 32,
  drumScale: 0.9,
  concept: 'One melodic line handed from the thin upper pulse to the warm lower one mid-phrase, so the join between the two lanes is inaudible.',
  dramatic: 'Analytical side hands to operational side. The melody crosses channels with a one-step overlap and descends as it goes: the register motion is the direction of travel.',
  safety: 'The 12.5% duty voice is the brightest thing in the suite, so it is also the shortest-lived: four notes per hand-off, never sustained, and silent for the whole last eight bars.',
  landmarks: [
    { id: 'intro', label: 'Intro — the source, up in the thin register', part: 'intro', bar: 0 },
    { id: 'a', label: 'A — first hand-off, thin to warm', part: 'loop', bar: 0 },
    { id: 'b', label: "A' — hand-off with the lydian B on the way in", part: 'loop', bar: 8 },
    { id: 'c', label: 'B — the hook complete, one voice all the way through', part: 'loop', bar: 16 },
    { id: 'd', label: 'C — landed: the app holds it alone', part: 'loop', bar: 24 },
  ],
  blocks: [
    { id: 'intro', part: 'intro', startBar: 0, ...voices({ lead: { volume: 0.0150 }, p2: { duty: 0.125, volume: 0.0072 }, tri: { volume: 0.031 } }) },
    { id: 'a', part: 'loop', startBar: 0, ...voices({ lead: { volume: 0.0160 }, p2: { duty: 0.125, volume: 0.0076 } }) },
    { id: 'b', part: 'loop', startBar: 8, ...voices({ lead: { volume: 0.0164 }, p2: { duty: 0.125, volume: 0.0078 } }) },
    { id: 'c', part: 'loop', startBar: 16, ...voices({ lead: { volume: 0.0172 }, p2: { duty: 0.25, volume: 0.0092 }, tri: { volume: 0.0335 } }) },
    { id: 'd', part: 'loop', startBar: 24, ...voices({ lead: { volume: 0.0150 }, p2: { duty: 0.5, volume: 0.0082 }, tri: { volume: 0.0305 } }) },
  ],
  holds: {
    // The hand-off notes: pulse 1 picks the line up and holds it, which is
    // what makes the overlap read as one instrument rather than two.
    'loop:1:12': { steps: 20 },
    'loop:9:12': { steps: 20 },
    'loop:19:0': { steps: 22, vib: { rate: 3.6, cents: 12, delay: 0.5 } },
    'loop:27:0': { steps: 26 },
  },
  dynamics: {
    intro: [[0, 0.58], [4, 0.8]],
    loop: [[0, 0.72], [8, 0.84], [16, 1.0], [22, 1.0], [24, 0.8], [30, 0.68], [32, 0.68]],
  },
  score: {
    intro: {
      pulse1: bars(R, R, R, bar('. . . . | . . . . | . . . . | C4 . . .')),
      pulse2: bars(
        R,
        bar('. . . . | . . . . | F4 . . . | . . . .'),
        bar('. . . . | E4 . . . | . . . . | C4 . . .'),
        bar('. . . . | . . . . | x . . . | . . . .'),
      ),
      p2mode: ['silent', 'melody', 'melody', 'melody'],
      arp: {},
      triangle: bars(
        bar('F2 . . . | . . . . | . . . . | . . . .'),
        R,
        bar('C3 . . . | . . . . | . . . . | . . . .'),
        R,
      ),
      noise: bars(
        drums('B . . . | . . . . | . . . . | . . . .'),
        RD, RD,
        drums('. . . . | . . . . | . . . . | S . . .'),
      ),
    },
    loop: {
      // F | Bb | Dm | C, two bars each.
      pulse1: bars(
        // A — pulse 1 takes the line at bar 2 step 12 and holds it.
        R,
        bar('. . . . | . . . . | . . . . | A3 . . .'),
        bar('. . . . | . . . . | F3 . . . | . . . .'),
        bar('. . . . | . . x . | . . . . | . . . .'),
        R,
        bar('. . . . | . . . . | . . . . | G3 . . .'),
        bar('. . . . | . . . . | F3 . . . | . . . .'),
        bar('. . . . | x . . . | . . . . | . . . .'),
        // A' — same shape, one degree higher on the way out.
        R,
        bar('. . . . | . . . . | . . . . | A3 . . .'),
        bar('. . . . | . . . . | G3 . . . | . . . .'),
        bar('. . . . | . . x . | . . . . | . . . .'),
        R,
        bar('. . . . | . . . . | . . . . | Bb3 . . .'),
        bar('. . . . | . . . . | A3 . . . | . . . .'),
        bar('. . . . | x . . . | . . . . | . . . .'),
        // B — the complete hook, F4 C4 E4 A4, one voice the whole way.
        bar('. . . . | . . . . | . . . . | C4 . . .'),
        bar('F4 . . . | . . . . | C4 . . . | E4 . . .'),
        R,
        bar('A4 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . x . | . . . . | . . . .'),
        bar('. . . . | . . . . | G4 . . . | . . . .'),
        bar('F4 . . . | . . . . | . . . . | x . . .'),
        R,
        // C — landed. The app holds the note; nothing else moves.
        bar('. . . . | . . . . | C4 . . . | . . . .'),
        bar('. . . . | x . . . | . . . . | . . . .'),
        R,
        bar('F3 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | . . x . | . . . .'),
        R,
        bar('. . . . | . . . . | C4 . . . | . . . .'),
        bar('. . . . | . . x . | . . . . | . . . .'),
      ),
      // The analytical side: it starts each phrase and lets go.
      pulse2: bars(
        R,
        bar('F4 . . . | . . . . | E4 . . . | C4 . . .'),
        bar('. . x . | . . . . | . . . . | . . . .'),
        R,
        R,
        bar('F4 . . . | . . . . | D4 . . . | Bb3 . . .'),
        bar('. . x . | . . . . | . . . . | . . . .'),
        R,
        R,
        bar('A4 . . . | . . . . | B4 . . . | C4 . . .'),
        bar('. . x . | . . . . | . . . . | . . . .'),
        R,
        R,
        bar('A4 . . . | . . . . | B4 . . . | D4 . . .'),
        bar('. . x . | . . . . | . . . . | . . . .'),
        R,
        R, R, R, R, R, R, R, R,
        R, R, R, R, R, R, R, R,
      ),
      p2mode: [
        'silent', 'melody', 'melody', 'silent', 'silent', 'melody', 'melody', 'silent',
        'silent', 'melody', 'melody', 'silent', 'silent', 'melody', 'melody', 'silent',
        'arp', 'silent', 'arp', 'silent', 'arp', 'silent', 'arp', 'silent',
        'arp', 'silent', 'silent', 'arp', 'silent', 'silent', 'arp', 'silent',
      ],
      arp: {
        16: [['F3', 'A3', 'C4']],
        18: [['Bb2', 'D3', 'F3']],
        20: [['D3', 'F3', 'A3']],
        22: [['C3', 'E3', 'G3']],
        24: [['F3', 'A3', 'C4']],
        27: [['Bb2', 'D3', 'F3']],
        30: [['C3', 'E3', 'G3']],
      },
      triangle: bars(
        bar('F2 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | C3 . . . | . . . .'),
        bar('F2 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | A2 . . . | . . . .'),
        bar('Bb2 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | F2 . . . | . . . .'),
        bar('Bb2 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | D3 . . . | . . . .'),
        bar('F2 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | C3 . . . | . . . .'),
        bar('F2 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | E2 . . . | . . . .'),
        bar('Bb2 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | F2 . . . | . . . .'),
        bar('Bb2 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | C3 . . . | . . . .'),
        // The lament in the major mode: F-E-D-C.
        bar('F2 . . . | . . . . | E2 . . . | . . . .'),
        bar('D3 . . . | . . . . | C3 . . . | . . . .'),
        bar('Bb2 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | F2 . . . | . . . .'),
        bar('D3 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | A2 . . . | . . . .'),
        bar('C3 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | G2 . . . | . . . .'),
        bar('F2 . . . | . . . . | . . . . | . . . .'),
        R,
        bar('Bb2 . . . | . . . . | . . . . | . . . .'),
        R,
        bar('D3 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | A2 . . . | . . . .'),
        bar('C3 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | G2 . . . | . . . .'),
      ),
      noise: bars(
        drums('B . . . | . . . . | . . . . | . . . .'),
        RD, RD,
        drums('. . . . | . . . . | . . . . | r . . .'),
        drums('b . . . | . . . . | . . . . | . . . .'),
        RD, RD, RD,
        drums('B . . . | . . . . | . . . . | . . . .'),
        RD, RD,
        drums('. . . . | . . . . | . . . . | w . . .'),
        drums('b . . . | . . . . | . . . . | . . . .'),
        RD, RD,
        drums('. . . . | . . . . | . . . . | S . . .'),
        drums('B . . . | . . . . | . . . . | . . . .'),
        RD,
        drums('. . . . | . . . . | T . . . | . . . .'),
        RD,
        drums('b . . . | . . . . | . . . . | . . . .'),
        RD,
        drums('. . . . | . . . . | t . . . | . . . .'),
        drums('. . . . | . . . . | . . . . | w . . .'),
        drums('b . . . | . . . . | . . . . | . . . .'),
        RD, RD, RD,
        drums('S . . . | . . . . | . . . . | . . . .'),
        RD, RD, RD,
      ),
    },
  },
}

/* ========================================================================== *
 * ROUND 5 — "Hold the Line"
 * survive_connection_spike · D minor over its dominant · 84 BPM
 * --------------------------------------------------------------------------
 * DRAMATIC READ: sixty-four connections arrive at once and nothing flinches.
 * The proof is not speed, it is that the shape of the thing does not change
 * under load.
 *
 * So the cue is built on one note that never moves: A2, the dominant, for all
 * thirty-two bars, never re-voiced, never transposed. The spike is bars 17-24
 * and it is a SWELL, not an acceleration — the dynamics climb about 4 dB and
 * the pad thickens from two notes to three, while the number of events per
 * bar stays exactly what it was. The melody quotes Round 1 note for note
 * (D4 A3 C4 F4) but over the dominant pedal it never resolves, which is what
 * held load sounds like.
 * ========================================================================== */

const R5: RoundCue = {
  id: 'r5',
  round: 5,
  roundId: 'survive_connection_spike',
  title: 'Get spike-ready',
  musicTitle: 'Hold the Line',
  key: 'D minor over a dominant A pedal',
  bpm: 84,
  master: ROUND_THEME_VOLUME,
  introBars: 4,
  loopBars: 32,
  drumScale: 0.95,
  concept: 'One pedal note for ninety seconds. The spike is a swell, not an acceleration — the event count per bar never changes.',
  dramatic: 'Round 1 material held over the dominant so it never resolves. Bars 17-24 are the load: four decibels louder, one voice thicker, exactly the same number of notes.',
  safety: 'Nothing accelerates, ever. The swell is carried by the triangle and the low pad, not by anything bright, and it releases into the four quietest bars of the cue.',
  landmarks: [
    { id: 'intro', label: 'Intro — the pedal, alone', part: 'intro', bar: 0 },
    { id: 'a', label: 'A — baseline: the hook, unresolved', part: 'loop', bar: 0 },
    { id: 'b', label: "A' — the pad joins, still no motion in the bass", part: 'loop', bar: 8 },
    { id: 'c', label: 'B — THE SPIKE: louder and thicker, not faster', part: 'loop', bar: 16 },
    { id: 'd', label: 'C — released, and the pedal is still there', part: 'loop', bar: 24 },
  ],
  blocks: [
    { id: 'intro', part: 'intro', startBar: 0, ...voices({ lead: { volume: 0.0145 }, tri: { volume: 0.0315 } }) },
    { id: 'a', part: 'loop', startBar: 0, ...voices({ lead: { volume: 0.0155 }, p2: { volume: 0.0090 }, tri: { volume: 0.0325 } }) },
    { id: 'b', part: 'loop', startBar: 8, ...voices({ lead: { volume: 0.0158 }, p2: { volume: 0.0096 }, tri: { volume: 0.0330 } }) },
    { id: 'c', part: 'loop', startBar: 16, ...voices({ lead: { duty: 0.25, volume: 0.0175 }, p2: { volume: 0.0112 }, tri: { volume: 0.0355 } }) },
    { id: 'd', part: 'loop', startBar: 24, ...voices({ lead: { volume: 0.0148 }, p2: { volume: 0.0086 }, tri: { volume: 0.0305 } }) },
  ],
  holds: {
    'loop:19:0': { steps: 24, vib: { rate: 3.3, cents: 15, delay: 0.4 } },
    'loop:23:0': { steps: 16, vib: { rate: 3.3, cents: 10, delay: 0.5 } },
  },
  dynamics: {
    intro: [[0, 0.52], [4, 0.74]],
    // The spike, as a curve: up over four bars, held for four, released over
    // four. Nothing in the note data changes across it.
    loop: [[0, 0.66], [8, 0.72], [16, 0.86], [20, 1.06], [24, 0.9], [27, 0.66], [32, 0.66]],
  },
  score: {
    intro: {
      pulse1: bars(R, R, R, bar('. . . . | . . . . | D4 . . . | . . . .')),
      pulse2: [],
      p2mode: ['silent', 'silent', 'silent', 'silent'],
      arp: {},
      triangle: bars(bar('A2 . . . | . . . . | . . . . | . . . .'), R, R, R),
      noise: bars(
        drums('B . . . | . . . . | . . . . | . . . .'),
        RD,
        drums('b . . . | . . . . | . . . . | . . . .'),
        drums('. . . . | . . . . | . . . . | S . . .'),
      ),
    },
    loop: {
      pulse1: bars(
        // A — the Round 1 hook, over the wrong bass.
        bar('D4 . . . | . . . . | A3 . . . | . . . .'),
        bar('. . . . | x . . . | . . . . | . . . .'),
        R,
        bar('. . . . | . . . . | C4 . . . | . . . .'),
        bar('. . . . | . . x . | . . . . | . . . .'),
        R,
        bar('D4 . . . | . . . . | A3 . . . | . . . .'),
        bar('. . . . | x . . . | . . . . | . . . .'),
        bar('D4 . . . | . . . . | A3 . . . | C4 . . .'),
        bar('. . . . | . . . . | x . . . | . . . .'),
        R,
        bar('. . . . | . . . . | F4 . . . | . . . .'),
        bar('. . . . | . . x . | . . . . | . . . .'),
        R,
        bar('. . . . | . . . . | C4 . . . | . . . .'),
        bar('. . . . | x . . . | . . . . | . . . .'),
        // B — the spike. Same rhythm as A, four decibels up.
        bar('D4 . . . | . . . . | A3 . . . | C4 . . .'),
        R,
        bar('. . x . | . . . . | . . . . | . . . .'),
        bar('F4 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . x . | . . . . | . . . .'),
        R,
        bar('D4 . . . | . . . . | A3 . . . | . . . .'),
        bar('E4 . . . | . . . . | . . . . | x . . .'),
        // C — released.
        bar('. . . . | . . . . | D4 . . . | . . . .'),
        bar('. . . . | x . . . | . . . . | . . . .'),
        R,
        bar('. . . . | . . . . | A3 . . . | . . . .'),
        bar('. . . . | . . x . | . . . . | . . . .'),
        R,
        R,
        R,
      ),
      pulse2: [],
      p2mode: [
        'silent', 'silent', 'silent', 'silent', 'silent', 'silent', 'silent', 'silent',
        'arp', 'arp', 'arp', 'arp', 'silent', 'silent', 'arp', 'arp',
        'arp', 'arp', 'arp', 'arp', 'arp', 'arp', 'arp', 'arp',
        'arp', 'arp', 'silent', 'silent', 'arp', 'silent', 'silent', 'silent',
      ],
      // Two notes per pad in the baseline, three in the spike: the texture
      // thickens without a single extra event.
      arp: {
        8: [['D3', 'A3']],
        9: [['D3', 'A3']],
        10: [['D3', 'G3']],
        11: [['D3', 'G3']],
        14: [['E3', 'A3']],
        15: [['E3', 'A3']],
        16: [['D3', 'F3', 'A3']],
        17: [['D3', 'F3', 'A3']],
        18: [['D3', 'G3', 'Bb3']],
        19: [['D3', 'G3', 'Bb3']],
        20: [['D3', 'F3', 'A3']],
        21: [['D3', 'F3', 'A3']],
        22: [['Cs3', 'E3', 'A3']],
        23: [['Cs3', 'E3', 'A3']],
        24: [['D3', 'F3', 'A3']],
        25: [['D3', 'F3', 'A3']],
        28: [['E3', 'A3']],
      },
      // A2. Thirty-two bars. Re-articulated every two bars and never once
      // moved, which is the whole point of the cue.
      triangle: x(16, bars(bar('A2 . . . | . . . . | . . . . | . . . .'), R)),
      noise: bars(
        drums('B . . . | . . . . | . . . . | . . . .'),
        RD, RD, RD,
        drums('b . . . | . . . . | . . . . | . . . .'),
        RD, RD, RD,
        drums('B . . . | . . . . | . . . . | . . . .'),
        RD, RD, RD,
        drums('b . . . | . . . . | . . . . | . . . .'),
        RD, RD,
        drums('. . . . | . . . . | . . . . | S . . .'),
        drums('B . . . | . . . . | . . . . | . . . .'),
        RD, RD, RD,
        drums('B . . . | . . . . | . . . . | . . . .'),
        RD, RD,
        drums('. . . . | . . . . | . . . . | W . . .'),
        drums('b . . . | . . . . | . . . . | . . . .'),
        RD, RD, RD,
        drums('S . . . | . . . . | . . . . | . . . .'),
        RD, RD, RD,
      ),
    },
  },
}

/* ========================================================================== *
 * ROUND 6 — "Last Bell"
 * analyze_live_orders_without_slowing_checkout · D major · 80 BPM
 * --------------------------------------------------------------------------
 * DRAMATIC READ: the last round, and the one that hands over to a summary
 * page. It has to feel like an ending, and then it has to be able to sit
 * under a page of text indefinitely without asking for attention.
 *
 * D major is the title theme's parallel major and its once-per-session
 * arrival, so the suite ends where the soundtrack began, brightened. The hook
 * is stated in its major form — D4 A3 C#4 F#4, exactly the credits-cue
 * callback an octave down — plainly and early, because this is the one cue
 * where the audience should recognise it. Bars 9-16 give it a real cadence,
 * bars 17-24 are a plagal descent that reads as credits rolling, and bars
 * 25-32 are a coda thin enough to talk over forever. The loop seam therefore
 * joins the coda back to the hook, like a hymn's second verse.
 * ========================================================================== */

const R6: RoundCue = {
  id: 'r6',
  round: 6,
  roundId: 'analyze_live_orders_without_slowing_checkout',
  title: 'Move live application data into the lakehouse',
  musicTitle: 'Last Bell',
  key: 'D major',
  bpm: 80,
  master: ROUND_THEME_VOLUME,
  introBars: 4,
  loopBars: 32,
  drumScale: 0.95,
  concept: 'The parallel major of the title theme, stating the hook as D A C# F# — the credits callback, an octave down — then a coda thin enough to live under a summary page.',
  dramatic: 'The ending. A real cadence at bar 13, a plagal descent that reads as credits rolling, and eight bars of coda that can hold a summary screen indefinitely.',
  safety: 'The coda — a quarter of the loop — is triangle plus one pulse note every two bars. It is the quietest music in the suite and it is where the summary page lands.',
  landmarks: [
    { id: 'intro', label: 'Intro — the bell answered in the major', part: 'intro', bar: 0 },
    { id: 'a', label: 'A — the hook, plainly: D A C# F#', part: 'loop', bar: 0 },
    { id: 'b', label: "A' — harmonised, and a real cadence", part: 'loop', bar: 8 },
    { id: 'c', label: 'B — the plagal descent: credits rolling', part: 'loop', bar: 16 },
    { id: 'd', label: 'C — the coda, for the summary page', part: 'loop', bar: 24 },
  ],
  blocks: [
    { id: 'intro', part: 'intro', startBar: 0, ...voices({ lead: { volume: 0.0150 }, p2: { volume: 0.0090 }, tri: { volume: 0.0315 } }) },
    { id: 'a', part: 'loop', startBar: 0, ...voices({ lead: { volume: 0.0166 }, p2: { volume: 0.0096 } }) },
    { id: 'b', part: 'loop', startBar: 8, ...voices({ lead: { volume: 0.0170 }, p2: { duty: 0.25, volume: 0.0100 } }) },
    { id: 'c', part: 'loop', startBar: 16, ...voices({ lead: { volume: 0.0172 }, p2: { volume: 0.0104 }, tri: { volume: 0.0335 } }) },
    { id: 'd', part: 'loop', startBar: 24, ...voices({ lead: { volume: 0.0140 }, p2: { volume: 0.0080 }, tri: { volume: 0.0295 } }) },
  ],
  holds: {
    'loop:1:0': { steps: 22, vib: { rate: 3.5, cents: 12, delay: 0.5 } },
    'loop:9:0': { steps: 20, vib: { rate: 3.5, cents: 12, delay: 0.5 } },
    // The cadence: a long F#4 over the A chord resolving to D.
    'loop:13:0': { steps: 26, vib: { rate: 3.2, cents: 14, delay: 0.4 } },
    'loop:29:0': { steps: 30 },
  },
  dynamics: {
    intro: [[0, 0.58], [4, 0.84]],
    loop: [[0, 0.86], [8, 0.94], [13, 1.04], [16, 0.94], [22, 0.86], [24, 0.66], [30, 0.6], [32, 0.6]],
  },
  echoSteps: 2,
  score: {
    intro: {
      pulse1: bars(R, R, bar('. . . . | . . . . | D4 . . . | . . . .'), bar('. . . . | . . . . | A3 . . . | . . . .')),
      pulse2: [],
      p2mode: ['silent', 'silent', 'silent', 'arp'],
      arp: { 3: [['D3', 'Fs3', 'A3']] },
      triangle: bars(
        bar('D3 . . . | . . . . | . . . . | . . . .'),
        R,
        bar('A2 . . . | . . . . | . . . . | . . . .'),
        R,
      ),
      noise: bars(
        drums('B . . . | . . . . | . . . . | . . . .'),
        RD, RD,
        drums('. . . . | . . . . | . . . . | S . . .'),
      ),
    },
    loop: {
      // D | G | Bm | A, two bars each.
      pulse1: bars(
        // A — the hook, complete, first time: D4 A3 C#4 F#4.
        bar('D4 . . . | . . . . | A3 . . . | Cs4 . . .'),
        bar('Fs4 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . x . | . . . . | . . . .'),
        bar('. . . . | . . . . | E4 . . . | . . . .'),
        bar('D4 . . . | . . . . | . . . . | x . . .'),
        R,
        bar('. . . . | . . . . | A3 . . . | B3 . . .'),
        bar('. . . . | . . . . | x . . . | . . . .'),
        // A' — harmonised, then the cadence at bar 13.
        bar('D4 . . . | . . . . | A3 . . . | Cs4 . . .'),
        bar('Fs4 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | x . . . | . . . .'),
        bar('. . . . | . . . . | G4 . . . | . . . .'),
        bar('. . . . | x . . . | . . . . | E4 . . .'),
        bar('Fs4 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | . . . . | . . . .'),
        bar('. . x . | . . . . | . . . . | . . . .'),
        // B — the plagal descent. Two-note gestures, credits rolling.
        bar('. . . . | . . . . | D4 . . . | . . . .'),
        bar('. . . . | . . . . | Cs4 . . . | . . . .'),
        bar('. . . . | . . . . | B3 . . . | . . . .'),
        bar('. . . . | . . . . | A3 . . . | . . . .'),
        bar('. . . . | . . x . | . . . . | . . . .'),
        bar('. . . . | . . . . | Fs4 . . . | . . . .'),
        bar('E4 . . . | . . . . | . . . . | . . . .'),
        bar('D4 . . . | . . . . | . . . . | x . . .'),
        // C — the coda. One note every two bars.
        R,
        bar('. . . . | . . . . | A3 . . . | . . . .'),
        bar('. . . . | . . x . | . . . . | . . . .'),
        R,
        R,
        bar('D4 . . . | . . . . | . . . . | . . . .'),
        R,
        bar('. . . . | . . . . | . . x . | . . . .'),
      ),
      pulse2: [],
      // The pad takes bars off. A finale can afford more presence than the
      // other five cues, but not a continuous chord.
      p2mode: [
        'arp', { kind: 'echo', gain: 0.5, maxGate: 10 }, 'silent', 'arp',
        'arp', 'silent', { kind: 'echo', gain: 0.5, maxGate: 10 }, 'silent',
        'arp', { kind: 'echo', gain: 0.52, maxGate: 10 }, 'silent', 'arp',
        'arp', { kind: 'echo', gain: 0.52, maxGate: 10 }, 'silent', 'arp',
        'arp', 'silent', 'arp', 'silent',
        'arp', 'silent', { kind: 'echo', gain: 0.48, maxGate: 10 }, 'arp',
        'arp', 'silent', 'silent', 'arp', 'silent', 'silent', 'arp', 'arp',
      ],
      arp: {
        0: [['D3', 'Fs3', 'A3']],
        3: [['D3', 'Fs3', 'A3']],
        4: [['G2', 'B2', 'D3']],
        5: [['G2', 'B2', 'D3']],
        7: [['A2', 'Cs3', 'E3']],
        8: [['D3', 'Fs3', 'A3']],
        10: [['G2', 'B2', 'D3']],
        11: [['G2', 'B2', 'D3']],
        12: [['A2', 'Cs3', 'E3']],
        14: [['A2', 'Cs3', 'E3']],
        15: [['D3', 'Fs3', 'A3']],
        16: [['D3', 'Fs3', 'A3']],
        17: [['D3', 'Fs3', 'A3']],
        18: [['G2', 'B2', 'D3']],
        19: [['G2', 'B2', 'D3']],
        20: [['B2', 'D3', 'Fs3']],
        21: [['B2', 'D3', 'Fs3']],
        23: [['A2', 'Cs3', 'E3']],
        24: [['D3', 'Fs3', 'A3']],
        27: [['G2', 'B2', 'D3']],
        30: [['A2', 'Cs3', 'E3']],
        // Bar 32 keeps the dominant sounding into the seam, so the return to
        // the hook is a resolution rather than a restart.
        31: [['A2', 'Cs3', 'E3']],
      },
      triangle: bars(
        bar('D3 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | A2 . . . | . . . .'),
        bar('D3 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | Fs2 . . . | . . . .'),
        bar('G2 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | D3 . . . | . . . .'),
        bar('B2 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | A2 . . . | . . . .'),
        bar('D3 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | A2 . . . | . . . .'),
        bar('G2 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | B2 . . . | . . . .'),
        bar('A2 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | Cs3 . . . | . . . .'),
        bar('D3 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | A2 . . . | . . . .'),
        // The lament in the major mode: D-C#-B-A.
        bar('D3 . . . | . . . . | Cs3 . . . | . . . .'),
        bar('B2 . . . | . . . . | A2 . . . | . . . .'),
        bar('G2 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | D3 . . . | . . . .'),
        bar('B2 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | Fs2 . . . | . . . .'),
        bar('A2 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | E2 . . . | . . . .'),
        bar('D3 . . . | . . . . | . . . . | . . . .'),
        R,
        bar('G2 . . . | . . . . | . . . . | . . . .'),
        R,
        bar('D3 . . . | . . . . | . . . . | . . . .'),
        R,
        bar('A2 . . . | . . . . | . . . . | . . . .'),
        bar('. . . . | . . . . | . . . . | . . . .'),
      ),
      noise: bars(
        drums('B . . . | . . . . | . . . . | . . . .'),
        RD, RD,
        drums('. . . . | . . . . | . . . . | w . . .'),
        drums('b . . . | . . . . | . . . . | . . . .'),
        RD,
        drums('. . . . | . . . . | t . . . | . . . .'),
        RD,
        drums('B . . . | . . . . | . . . . | . . . .'),
        RD, RD,
        drums('. . . . | . . . . | . . . . | w . . .'),
        drums('b . . . | . . . . | . . . . | . . . .'),
        RD, RD,
        drums('. . . . | . . . . | . . . . | S . . .'),
        drums('B . . . | . . . . | . . . . | . . . .'),
        RD,
        drums('. . . . | . . . . | T . . . | . . . .'),
        RD,
        drums('b . . . | . . . . | . . . . | . . . .'),
        RD,
        drums('. . . . | . . . . | t . . . | . . . .'),
        drums('. . . . | . . . . | . . . . | w . . .'),
        drums('B . . . | . . . . | . . . . | . . . .'),
        RD, RD, RD,
        drums('S . . . | . . . . | . . . . | . . . .'),
        RD, RD, RD,
      ),
    },
  },
}

/* -------------------------------------------------------------------------- *
 * The suite
 * -------------------------------------------------------------------------- */

export const ROUND_CUES: Readonly<Record<RoundId, RoundCue>> = {
  wake_idle_app: R1,
  make_schema_change_safely: R2,
  recover_deleted_order: R3,
  put_model_score_in_app: R4,
  survive_connection_spike: R5,
  analyze_live_orders_without_slowing_checkout: R6,
}

export const ROUND_CUE_LIST: readonly RoundCue[] = [R1, R2, R3, R4, R5, R6]

const plans = new Map<string, RoundPlan>()

/**
 * Plan a cue into two flat event lists: the intro once, then the body that
 * loops. Planning is pure and memoised, so the first start of a round pays for
 * it and every later start is free.
 */
export function roundCuePlan(roundId: RoundId): RoundPlan {
  const cached = plans.get(roundId)
  if (cached) return cached
  const cue = ROUND_CUES[roundId]
  const stepSeconds = 60 / cue.bpm / 4
  const barSeconds = stepSeconds * STEPS_PER_BAR
  const introDuration = cue.introBars * barSeconds
  const loopDuration = cue.loopBars * barSeconds
  const accents = cue.accents ?? DEFAULT_ACCENTS
  const plan: RoundPlan = {
    id: cue.id,
    bpm: cue.bpm,
    stepSeconds,
    barSeconds,
    introDuration,
    loopDuration,
    introEvents: partEvents(cue, 'intro', stepSeconds, introDuration, cue.dynamics.intro, accents),
    loopEvents: partEvents(cue, 'loop', stepSeconds, loopDuration, cue.dynamics.loop, accents),
    sections: cue.landmarks.map((landmark) => ({
      id: landmark.id,
      label: landmark.label,
      part: landmark.part,
      startBar: landmark.bar + 1,
      startSeconds: landmark.part === 'intro'
        ? landmark.bar * barSeconds
        : introDuration + landmark.bar * barSeconds,
    })),
  }
  plans.set(roundId, plan)
  return plan
}
