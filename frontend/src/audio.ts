import type { RoundId } from './api/types'
import { ROUND_THEME_VOLUME, roundCuePlan } from './round-score'
import { CREDITS_THEME_VOLUME, creditsCuePlan } from './credits-score'

/**
 * Bar 40's downbeat — the chord that ends the show, 85.091 s in.
 *
 * Re-exported so a roll can pin its own length to the music rather than the
 * other way round, which is what the standalone credits deliverable does. The
 * in-app roll does not, yet; see credits.tsx.
 */
export { CREDITS_THEME_BELL_SECONDS, CREDITS_THEME_DURATION } from './credits-score'

let sharedContext: AudioContext | null = null
let nextThemeToken = 0
let originalRoundThemeMuted = false
let originalTitleThemeMuted = false
let originalCreditsThemeMuted = false

const ORIGINAL_TITLE_THEME_VOLUME = 0.18

// The title score, the six round cues and the credits cue are three plans on
// one runtime; see CueRun below. They stay in separate slots rather than one,
// because they are mutually exclusive by policy rather than by structure and
// because playTone's ducking reads the title one specifically.
let activeTitleTheme: CueRun | null = null
let activeRoundTheme: CueRun | null = null
let activeCreditsTheme: CueRun | null = null

function safeDisconnect(node: AudioNode): void {
  try { node.disconnect() } catch { /* already disconnected */ }
}

function audioContext(): AudioContext | null {
  const AudioContextClass = window.AudioContext ?? window.webkitAudioContext
  if (!AudioContextClass) return null
  if (!sharedContext || sharedContext.state === 'closed') {
    sharedContext = new AudioContextClass()
  }
  if (sharedContext.state === 'suspended') void sharedContext.resume().catch(() => undefined)
  return sharedContext
}

// Keyed by duty so 12.5% / 25% / 50% pulses can coexist, and still built once
// per context per duty.
const pulseWaves = new WeakMap<AudioContext, Map<number, PeriodicWave>>()

function pulseWave(context: AudioContext, duty: number): PeriodicWave {
  let byDuty = pulseWaves.get(context)
  if (!byDuty) {
    byDuty = new Map()
    pulseWaves.set(context, byDuty)
  }
  const existing = byDuty.get(duty)
  if (existing) return existing
  const real = new Float32Array(33)
  const imaginary = new Float32Array(33)
  for (let harmonic = 1; harmonic < real.length; harmonic += 1) {
    const phase = Math.PI * 2 * harmonic * duty
    real[harmonic] = (2 * Math.sin(phase)) / (Math.PI * harmonic)
    imaginary[harmonic] = (2 * (1 - Math.cos(phase))) / (Math.PI * harmonic)
  }
  const wave = context.createPeriodicWave(real, imaginary, { disableNormalization: false })
  byDuty.set(duty, wave)
  return wave
}

type NoteStep = number | null

const N = null
const F2 = 87.31; const G2 = 98; const Ab2 = 103.83; const A2 = 110; const Bb2 = 116.54
const C3 = 130.81; const Cs3 = 138.59; const D3 = 146.83; const E3 = 164.81
const F3 = 174.61; const Fs3 = 185; const G3 = 196; const A3 = 220; const Bb3 = 233.08
const C4 = 261.63; const Cs4 = 277.18; const D4 = 293.66; const E4 = 329.63
const F4 = 349.23; const Fs4 = 369.99; const G4 = 392; const Ab4 = 415.3; const A4 = 440
const Bb4 = 466.16; const B4 = 493.88; const C5 = 523.25; const Cs5 = 554.37; const D5 = 587.33
const E5 = 659.25; const F5 = 698.46; const Fs5 = 739.99; const G5 = 783.99
const Ab5 = 830.61; const A5 = 880; const Bb5 = 932.33; const B5 = 987.77
const Cs6 = 1108.73; const D6 = 1174.66

/** Join bars into one flat step list. Scores are written a bar at a time. */
function phrase<T>(...sections: ReadonlyArray<readonly T[]>): readonly T[] {
  return sections.flat()
}

// ===========================================================================
// FINAL BELL — the title / attract score. 112 BPM, 4/4: an eight-bar intro
// (17.14 s) once, then a 28-bar body (60.00 s) forever. Two pulses, one
// triangle, one noise, and never more than four voices sounding at once.
//
// D minor, the B section in F major, a borrowed D-flat major at bar 20 and a
// borrowed B-flat minor at bar 24. THE HOOK is D5 · A4 · C5 · F5 — a quarter,
// a dotted eighth, a dotted eighth, and a dotted quarter that HOLDS, so all
// four notes of the contour carry weight and the fourth is the arrival.
//
// Every drum is one real 15-bit LFSR at a different timer period, so a kick
// and a hat physically cannot overlap and the noise channel stays one voice.
// The score is planned into a flat event list and played by the shared cue
// runtime below, which the six round cues in round-score.ts also use.
// ===========================================================================

/** One hit, one channel — never a pair. */
type TitleDrumStep =
  | 'K' | 'k' | 'S' | 's' | 'H' | 'h' | 'T' | 't' | 'C' | 'G' | 'R'
  | '1' | '2' | '3' | '4'
  | null

/** What the second pulse is doing in a given bar. This is the arrangement. */
type TitlePulse2Role = 'bell' | 'silent' | 'echo' | 'arp' | 'melody'

/** A duty envelope: [fractionThroughTheNote, duty] pairs, first fraction 0. */
type CueDutySegment = readonly [number, number]

interface CueVibrato {
  /** Hz. NES vibrato is a pitch table stepped once per video frame. */
  readonly rate: number
  /** Peak deviation in cents. */
  readonly cents: number
  /** Fraction of the note to wait before the vibrato starts. */
  readonly delay: number
}

interface TitleHoldSpec {
  /** Sounding length in sixteenths. Everything not held is staccato. */
  readonly steps: number
  readonly vib?: CueVibrato
  readonly duty?: readonly CueDutySegment[]
  /** Portamento origin — the note slides into its pitch from here. */
  readonly from?: number
}

interface TitleBlockShape {
  readonly part: 'intro' | 'loop'
  readonly startBar: number
  readonly lead: { duty: number; maxGate: number; attack: number; sustain: number; volume: number }
  readonly p2: { duty: number; maxGate: number; attack: number; sustain: number; volume: number }
  readonly tri: { maxGate: number; attack: number; sustain: number; volume: number }
}

interface CueEvent {
  readonly channel: 'pulse1' | 'pulse2' | 'triangle' | 'noise'
  readonly kind: 'note' | 'arp' | 'noise'
  readonly time: number
  readonly duration: number
  readonly gain: number
  readonly frequency?: number
  readonly chord?: readonly number[]
  readonly duty?: number
  readonly dutyEnvelope?: readonly CueDutySegment[] | null
  readonly vibrato?: CueVibrato | null
  readonly portamentoFrom?: number | null
  readonly attack?: number
  readonly sustain?: number
  readonly period?: number
  readonly rateFrom?: number
}

interface TitleThemePlan extends CuePlan {
  readonly stepSeconds: number
}

const TITLE_STEPS_PER_BAR = 16
const TITLE_INTRO_BARS = 8
const TITLE_LOOP_BARS = 28
const TITLE_BPM = 112

/** One NES video frame. Arpeggios, vibrato and pitch tables all run at this. */
const CUE_FRAME_SECONDS = 1 / 60

/**
 * The retrigger gap between adjacent notes in one channel. The single
 * exception is the last event in each channel of each part, which runs exactly
 * to the part boundary — half-open intervals, so the intro handoff and the
 * loop seam are gapless without ever overlapping.
 */
const TITLE_VOICE_GAP = 0.005

/**
 * The gate cut. A note stays audible until it is cut: an exponential run all
 * the way to -80 dB would spend the back of every gate inaudible, putting a
 * hole in front of every bar line. Instead the tail decays to -16 dB of
 * sustain and takes a 3.5 ms linear ramp to zero — short enough to read as a
 * hardware length counter, long enough to have no click in it.
 */
const CUE_RELEASE_SECONDS = 0.0035

/** Scheduling. 1.2 s of lookahead survives a backgrounded tab's 1 s timer. */
const CUE_LOOKAHEAD_SECONDS = 1.2
const CUE_SCHEDULER_MS = 140

/** THE HOOK. Quarter, dotted eighth, dotted eighth, held dotted quarter. */
const TITLE_HOOK: readonly NoteStep[] = [D5, N, N, N, A4, N, N, C5, N, N, F5, N, N, N, N, N]
const TITLE_ANSWER_DOWN: readonly NoteStep[] = [F5, N, E5, N, D5, N, N, C5, N, N, A4, N, N, N, N, N]
const TITLE_ANSWER_UP: readonly NoteStep[] = [F5, N, G5, N, A5, N, N, G5, N, N, E5, N, N, C5, N, N]

const TITLE_SILENT_BAR: readonly NoteStep[] = new Array<NoteStep>(TITLE_STEPS_PER_BAR).fill(N)

function titleSilentBars(count: number): readonly NoteStep[][] {
  return Array.from({ length: count }, () => [...TITLE_SILENT_BAR])
}

const TITLE_INTRO_PULSE1 = phrase<NoteStep>(
  [D6, N, N, N, N, N, N, N, N, N, N, N, N, N, N, N], // the bell's top partial
  TITLE_SILENT_BAR,
  [A4, N, D5, N, F5, N, A5, N, N, N, G5, N, F5, N, D5, N], // ring-announcer fanfare
  [E5, N, A5, N, G5, N, E5, N, Cs5, N, E5, N, A5, N, N, N],
  TITLE_HOOK,
  TITLE_ANSWER_DOWN,
  [Bb4, N, N, D5, N, N, G5, N, E5, N, Cs5, N, E5, N, G5, N],
  [D5, Fs5, A5, N, D6, N, N, N, N, N, N, N, Cs5, N, E5, G5], // D major, then A7
)

const TITLE_INTRO_PULSE2 = phrase<NoteStep>(
  [A5, N, N, N, N, N, N, N, N, N, N, N, N, N, N, N], // the bell's fifth
  ...titleSilentBars(7),
)

const TITLE_INTRO_TRIANGLE = phrase<NoteStep>(
  [D3, N, N, N, N, N, N, N, N, N, N, N, N, N, N, N], // the bell's fundamental
  TITLE_SILENT_BAR,
  [D3, N, N, N, D3, N, D3, N, A3, N, N, N, D3, N, C3, N],
  [A2, N, N, N, A2, N, A2, N, E3, N, N, N, A3, N, G3, N],
  [D3, N, N, N, D3, N, D3, N, A3, N, N, N, D3, N, C3, N],
  [Bb2, N, N, N, Bb2, N, Bb2, N, F3, N, N, N, F3, N, E3, N],
  [G2, N, N, N, G2, N, D3, N, A2, N, N, N, A2, N, Cs3, N],
  [D3, N, Fs3, N, A3, N, Fs3, N, N, N, N, N, A2, N, N, N],
)

const TITLE_INTRO_NOISE = phrase<TitleDrumStep>(
  ['G', N, N, N, N, N, N, N, N, N, N, N, N, N, N, N],
  ['1', N, N, N, '1', N, N, N, '2', N, '2', N, '3', N, '4', '4'],
  ['K', N, 'h', N, 'S', N, 'h', N, 'K', N, 'h', N, 'S', N, 'h', 'H'],
  ['K', N, 'h', N, 'S', N, 'h', N, 'K', 'k', 'h', N, 'S', 'R', 'S', 'H'],
  ['T', N, N, N, N, N, N, N, 'C', N, N, N, N, N, N, N],
  ['T', N, N, N, 'T', N, N, N, 'C', N, 'h', N, 'T', N, 'h', 'h'],
  ['K', N, 'h', N, 'S', N, 'h', N, 'K', N, 'h', N, 'S', 's', 'h', 'H'],
  ['G', N, N, N, N, N, 'h', N, 'K', N, 'h', N, 'S', 'R', 'S', 'H'],
)

const TITLE_LOOP_PULSE1 = phrase<NoteStep>(
  // A — Dm | Bb-F | Gm | A7 ‖ Dm | Bb-C | Gm-A7 | Dm
  TITLE_HOOK,
  TITLE_ANSWER_DOWN,
  [Bb4, N, N, D5, N, N, F5, N, G5, N, N, N, F5, N, D5, N],
  [E5, N, Cs5, N, E5, N, G5, N, A5, N, N, N, G5, N, E5, N],
  TITLE_HOOK,
  TITLE_ANSWER_UP,
  [D5, N, F5, N, G5, N, F5, N, E5, N, Cs5, N, E5, N, G5, N],
  [A5, N, N, F5, N, D5, N, N, A4, N, D5, N, F5, N, A5, N],
  // A' — Dm | F | Bb-Gm | A7 ‖ Dm | Bb-F | Dm-Gm | Gm-C7
  TITLE_HOOK,
  TITLE_HOOK, // identical pitches over F major: 13-3-5-1
  [D5, N, F5, N, Bb5, N, N, A5, G5, N, F5, N, D5, N, Bb4, N],
  [Cs5, N, E5, N, A5, N, N, G5, N, N, E5, N, Cs5, N, E5, N], // deceptive cadence
  TITLE_HOOK,
  TITLE_ANSWER_DOWN,
  [D5, N, A4, N, D5, N, F5, N, G5, N, Bb5, N, A5, N, G5, N],
  [F5, N, D5, N, Bb4, N, C5, N, E5, N, G5, N, E5, N, C5, N], // ii-V pivot to F
  // B — F major, shuffled 3+1, pulse 1 takes the front half of each bar
  [F5, N, N, E5, C5, N, N, A4, N, N, N, N, N, N, N, N],
  [E5, N, N, G5, E5, N, N, C5, N, N, N, N, N, N, N, N],
  [D5, N, N, F5, Bb5, N, N, A5, N, N, N, N, N, N, N, N],
  [Ab5, N, N, F5, Cs5, N, N, Ab4, Cs5, N, N, F5, Ab5, N, N, F5], // borrowed Db
  [G5, N, N, E5, C5, N, N, Bb4, N, N, N, N, N, N, N, N],
  [A5, N, N, F5, C5, N, N, A4, N, N, N, N, N, N, N, N],
  // Bridge — the trap, B-flat minor, the climb, the held A5
  [D4, N, N, N, N, N, N, N, F4, N, N, N, A4, N, N, N],
  [Bb4, N, N, N, Cs5, N, N, N, F5, N, N, Cs5, Bb4, N, N, N],
  [G4, N, Bb4, N, D5, N, F5, N, E5, N, Cs5, N, E5, N, G5, N],
  [A5, N, N, N, N, N, N, N, N, N, N, N, N, N, N, N],
  // Turnaround — fanfare, then the chromatic rip into bar 1's D
  [A5, N, G5, N, E5, N, Cs5, N, E5, N, G5, N, A5, N, N, N],
  [Cs6, N, N, N, A5, N, G5, N, E5, N, N, A4, Bb4, B4, C5, Cs5],
)

const TITLE_LOOP_PULSE2 = phrase<NoteStep>(
  // A: arpeggio-chord, generated. A': echo of pulse 1, generated.
  ...titleSilentBars(16),
  // B: hand-off. Pulse 2 answers in the back half with long gates, so its
  // first note overlaps pulse 1's last — legato one channel cannot play.
  [N, N, N, N, N, N, N, N, C5, N, N, F5, E5, N, N, C5],
  [N, N, N, N, N, N, N, N, D5, N, N, F5, A5, N, N, F5],
  [N, N, N, N, N, N, N, N, G5, N, N, D5, F5, N, N, G5],
  [F5, N, N, Cs5, Ab4, N, N, F4, Ab4, N, N, Cs5, F5, N, N, Cs5], // thirds
  [N, N, N, N, N, N, N, N, C5, N, N, E5, G5, N, N, Bb5],
  [N, N, N, N, N, N, N, N, Cs5, N, N, E5, A5, N, N, G5],
  // Bridge: silent, then arpeggio-chord. Turnaround: echo, then written.
  ...titleSilentBars(5),
  [A5, N, N, N, E5, N, Cs5, N, A4, N, N, E4, F4, Fs4, G4, A4],
)

const TITLE_LOOP_TRIANGLE = phrase<NoteStep>(
  // A — the march. Stomp on 1 and 2, a push on the and-of-2, the fifth on 3,
  // a low neighbour on the and-of-4. The long walk to the ring.
  [D3, N, N, N, D3, N, D3, N, A3, N, N, N, D3, N, C3, N],
  [Bb2, N, N, N, Bb2, N, Bb2, N, F3, N, N, N, F3, N, E3, N],
  [G2, N, N, N, G2, N, G2, N, D3, N, N, N, G3, N, F3, N],
  [A2, N, N, N, A2, N, A2, N, E3, N, N, N, A3, N, G3, N],
  [D3, N, N, N, D3, N, D3, N, A3, N, N, D3, F3, N, C3, N],
  [Bb2, N, N, N, Bb2, N, Bb2, N, C3, N, N, N, C3, N, Bb2, N],
  [G2, N, N, N, G2, N, D3, N, A2, N, N, N, A2, N, Cs3, N],
  [D3, N, A2, N, D3, N, F3, N, A3, N, F3, N, D3, N, A2, N],
  // A' — walking eighths. The arrangement is the crescendo, not the gain.
  [D3, N, A2, N, D3, N, F3, N, A3, N, F3, N, D3, N, E3, N],
  [F3, N, C3, N, F3, N, A3, N, C3, N, A3, N, F3, N, G3, N],
  [Bb2, N, F3, N, Bb2, N, D3, N, G2, N, D3, N, G3, N, Bb3, N],
  [A2, N, E3, N, A2, N, Cs3, N, E3, N, G3, N, A3, N, G3, N],
  [D3, N, A2, N, D3, N, F3, N, A3, N, F3, N, D3, N, C3, N],
  [Bb2, N, F3, N, Bb2, N, D3, N, F3, N, C3, N, F3, N, A3, N],
  [D3, N, A2, N, D3, N, F3, N, G2, N, D3, N, G3, N, Bb3, N],
  [G2, N, D3, N, G2, N, Bb2, N, C3, N, G3, N, C3, N, Bb3, N],
  // B — TWO JOBS. Bass in the F2-C3 octave on the shuffle's long note, a
  // counter-line in the C4-F4 octave on its short note. The register gap does
  // the separating; the ear hears two players.
  [F2, N, N, C4, F2, N, N, A3, C3, N, N, F4, F2, N, N, E4],
  [C3, N, N, G3, C3, N, N, E4, D3, N, N, A3, D3, N, N, F4],
  [Bb2, N, N, F4, Bb2, N, N, D4, G2, N, N, Bb3, G2, N, N, D4],
  [Ab2, N, N, F4, Ab2, N, N, Cs4, Cs3, N, N, Ab4, Cs3, N, N, F4],
  [C3, N, N, G3, C3, N, N, Bb3, G2, N, N, E4, C3, N, N, G3],
  [F2, N, N, C4, F2, N, N, A3, A2, N, N, E4, A2, N, N, Cs4],
  // Bridge
  [D3, N, N, N, N, N, N, N, A2, N, N, N, N, N, N, N],
  [Bb2, N, N, N, F3, N, N, N, Bb2, N, N, N, Cs3, N, N, N],
  [G2, N, D3, N, G2, N, Bb2, N, A2, N, E3, N, A2, N, Cs3, N],
  [A2, N, A2, N, A2, N, A2, N, A2, N, A2, N, E3, N, G3, N],
  // Turnaround — an A7 climb on beat three, then the dominant as a PEDAL for
  // the whole of beat four, running exactly to the bar line. The bass never
  // lifts across the seam, which is what makes the join continuous.
  [A2, N, E3, N, A2, N, Cs3, N, E3, N, A3, N, G3, N, E3, N],
  [A2, N, E3, N, A2, N, G3, N, Cs3, E3, G3, A3, A2, N, N, N],
)

const TITLE_LOOP_NOISE = phrase<TitleDrumStep>(
  // A
  ['K', N, 'h', N, 'S', N, 'h', N, 'K', N, 'h', 'k', 'S', N, 'h', 'H'],
  ['K', N, 'h', N, 'S', N, 'h', N, 'K', N, 'h', N, 'S', 's', 'h', 'H'],
  ['K', N, 'h', N, 'S', N, 'h', 'k', 'K', N, 'h', N, 'S', N, 't', 'T'],
  ['K', N, 'h', N, 'S', N, 'h', N, 'K', 'k', 'h', N, 'S', 'R', 'S', 'H'],
  ['K', N, 'h', 'h', 'S', N, 'h', N, 'K', 'k', 'h', N, 'S', N, 't', 'H'],
  ['K', N, 'h', N, 'S', N, 'h', N, 'K', N, 'S', N, 'S', N, 'h', 'H'],
  ['K', N, 'h', N, 'S', N, 'h', 'k', 'K', N, 'h', N, 'S', 's', 'h', 'H'],
  ['K', N, 'h', N, 'S', N, 'K', N, 'S', N, 't', N, 'T', 'R', 'S', 'H'],
  // A'
  ['K', 'h', 'h', 'h', 'S', 'h', 'h', 'h', 'K', 'h', 'h', 'k', 'S', 'h', 'h', 'H'],
  ['K', 'h', 'h', 'h', 'S', 'h', 'h', 'h', 'K', 'h', 'h', 'h', 'S', 'h', 's', 'H'],
  ['K', 'h', 'h', 'h', 'S', 'h', 'k', 'h', 'K', 'h', 'h', 'h', 'S', 'h', 'h', 'H'],
  ['K', 'h', 'h', 'h', 'S', 'h', 'h', 'h', 'K', 'k', 'h', 'h', 'S', 'R', 'S', 'H'],
  ['K', 'h', 'h', 'h', 'S', 'h', 'h', 'h', 'K', 'h', 'h', 'k', 'S', 'h', 'h', 'H'],
  ['K', 'h', 'h', 'h', 'S', 'h', 'h', 'h', 'K', 'h', 'S', 'h', 'S', 'h', 'h', 'H'],
  ['K', 'h', 'h', 'h', 'S', 'h', 'k', 'h', 'K', 'h', 'h', 'h', 'S', 'h', 't', 'T'],
  ['K', 'h', 'K', 'h', 'S', 'h', 'h', 'h', 'K', 'h', 'S', 'h', 'S', 's', 'S', 'H'],
  // B — shuffled kit; bar 20 is the crowd stomp on the borrowed chord
  ['K', N, N, 'h', 'S', N, N, 'h', 'K', N, N, 'h', 'S', N, N, 'H'],
  ['K', N, N, 'h', 'S', N, N, 'k', 'K', N, N, 'h', 'S', N, N, 'H'],
  ['K', N, N, 'h', 'S', N, N, 'h', 'K', N, N, 't', 'S', N, N, 'T'],
  ['T', N, N, 'T', 'C', N, N, 'h', 'T', N, N, 'T', 'C', N, N, 'H'],
  ['K', N, N, 'h', 'S', N, N, 'h', 'K', N, N, 'h', 'S', N, N, 'H'],
  ['K', N, N, 'h', 'S', N, N, 'K', 'S', N, N, 's', 'S', 'R', 'S', 'H'],
  // Bridge — the trap, then the four-stage rising roll
  ['G', N, N, N, N, N, N, N, N, N, N, N, N, N, N, N],
  [N, N, N, N, 't', N, N, N, N, N, N, N, 'T', N, N, N],
  ['k', N, 'h', N, 's', N, 'h', N, 'K', N, 'h', N, 'S', N, 't', 'T'],
  ['1', '1', '1', '1', '2', '2', '2', '2', '3', '3', '3', '3', '4', '4', '4', '4'],
  // Turnaround
  ['K', 'h', 'S', 'h', 'K', 'h', 'S', 'h', 'K', 'h', 'S', 'h', 'S', 'R', 'S', 'H'],
  ['K', 'h', 'K', 'h', 'S', 'h', 'S', 'h', 'K', 'h', 'S', 'h', 'S', 'R', 'S', 'H'],
)

/**
 * Arpeggio-chords for pulse 2, two per bar. `null` means the bar uses a role
 * other than 'arp'.
 */
type TitleArpBar = readonly [readonly number[], readonly number[]] | null

const TITLE_INTRO_ARP: readonly TitleArpBar[] = [
  null, null, null, null,
  [[D4, F4, A4], [D4, F4, A4]],
  [[Bb3, D4, F4], [C4, F4, A4]],
  [[G3, Bb3, D4], [Cs4, E4, G4]],
  [[Fs4, A4, D5], [Cs4, E4, G4]],
]

const TITLE_LOOP_ARP: readonly TitleArpBar[] = [
  [[D4, F4, A4], [D4, F4, A4]],
  [[Bb3, D4, F4], [C4, F4, A4]],
  [[G3, Bb3, D4], [G3, Bb3, D4]],
  [[Cs4, E4, G4], [Cs4, E4, A4]],
  [[D4, F4, A4], [D4, F4, A4]],
  [[Bb3, D4, F4], [C4, E4, G4]],
  [[G3, Bb3, D4], [Cs4, E4, G4]],
  [[D4, F4, A4], [D4, F4, A4]],
  null, null, null, null, null, null, null, null,
  null, null, null, null, null, null,
  null,
  [[Bb3, Cs4, F4], [Bb3, Cs4, F4]],
  [[G3, Bb3, D4], [Cs4, E4, G4]],
  [[Cs4, E4, G4], [Cs4, E4, A4]],
  null, null,
]

const TITLE_INTRO_P2: readonly TitlePulse2Role[] = [
  'bell', 'silent', 'echo', 'echo', 'arp', 'arp', 'arp', 'arp',
]

const TITLE_LOOP_P2: readonly TitlePulse2Role[] = [
  'arp', 'arp', 'arp', 'arp', 'arp', 'arp', 'arp', 'arp',
  'echo', 'echo', 'echo', 'echo', 'echo', 'echo', 'echo', 'echo',
  'melody', 'melody', 'melody', 'melody', 'melody', 'melody',
  'silent', 'arp', 'arp', 'arp',
  'echo', 'melody',
]

/**
 * Arrangement per block. `lead.maxGate` in sixteenths is the whole
 * articulation story: the lead is clipped to about 1.5 steps however long its
 * slot, which keeps the staccato crispness. Only notes named in TITLE_HOLDS
 * sustain, and those are the arrival notes — which is what makes them sound
 * like arrivals.
 */
const TITLE_BLOCKS: readonly TitleBlockShape[] = [
  {
    part: 'intro', startBar: 0,
    lead: { duty: 0.5, maxGate: 15, attack: 0.004, sustain: 0.34, volume: 0.017 },
    p2: { duty: 0.25, maxGate: 15, attack: 0.006, sustain: 0.3, volume: 0.014 },
    tri: { maxGate: 27, attack: 0.012, sustain: 0.42, volume: 0.03 },
  },
  {
    part: 'intro', startBar: 2,
    lead: { duty: 0.5, maxGate: 1.35, attack: 0.004, sustain: 0.52, volume: 0.0195 },
    p2: { duty: 0.125, maxGate: 1.2, attack: 0.003, sustain: 0.4, volume: 0.0072 },
    tri: { maxGate: 2.2, attack: 0.01, sustain: 0.74, volume: 0.03 },
  },
  {
    part: 'intro', startBar: 4,
    lead: { duty: 0.5, maxGate: 1.55, attack: 0.005, sustain: 0.56, volume: 0.0195 },
    p2: { duty: 0.25, maxGate: 1.4, attack: 0.006, sustain: 0.5, volume: 0.0074 },
    tri: { maxGate: 2.2, attack: 0.01, sustain: 0.76, volume: 0.031 },
  },
  {
    part: 'loop', startBar: 0,
    lead: { duty: 0.5, maxGate: 1.55, attack: 0.005, sustain: 0.56, volume: 0.0192 },
    p2: { duty: 0.25, maxGate: 1.4, attack: 0.006, sustain: 0.5, volume: 0.0074 },
    tri: { maxGate: 2.2, attack: 0.01, sustain: 0.76, volume: 0.031 },
  },
  {
    part: 'loop', startBar: 8,
    lead: { duty: 0.5, maxGate: 1.55, attack: 0.005, sustain: 0.58, volume: 0.0196 },
    // The echo's gate outlasts the lead's on purpose — a delay tail rings past
    // the note that made it — and the walking bass is legato, so this section
    // has a continuous floor instead of eight thuds with holes between them.
    p2: { duty: 0.125, maxGate: 1.95, attack: 0.003, sustain: 0.4, volume: 0.0068 },
    tri: { maxGate: 2, attack: 0.008, sustain: 0.72, volume: 0.03 },
  },
  {
    part: 'loop', startBar: 16,
    lead: { duty: 0.25, maxGate: 2.4, attack: 0.005, sustain: 0.6, volume: 0.019 },
    p2: { duty: 0.25, maxGate: 3.2, attack: 0.006, sustain: 0.58, volume: 0.0145 },
    tri: { maxGate: 3, attack: 0.009, sustain: 0.74, volume: 0.029 },
  },
  {
    part: 'loop', startBar: 22,
    lead: { duty: 0.25, maxGate: 3.6, attack: 0.008, sustain: 0.66, volume: 0.02 },
    p2: { duty: 0.25, maxGate: 1.4, attack: 0.006, sustain: 0.5, volume: 0.0076 },
    tri: { maxGate: 3.4, attack: 0.011, sustain: 0.78, volume: 0.031 },
  },
  {
    part: 'loop', startBar: 26,
    lead: { duty: 0.5, maxGate: 1.45, attack: 0.004, sustain: 0.54, volume: 0.0202 },
    p2: { duty: 0.125, maxGate: 1.3, attack: 0.003, sustain: 0.42, volume: 0.0082 },
    // Legato bass for the same reason: the two bars that hand over to the seam
    // are the two that must not breathe.
    tri: { maxGate: 2, attack: 0.008, sustain: 0.72, volume: 0.031 },
  },
]

/** Held lead notes, keyed `part:bar:step`. Everything else is staccato. */
const TITLE_HOLDS: Record<string, TitleHoldSpec> = {
  'intro:0:0': { steps: 14, duty: [[0, 0.5], [0.35, 0.25], [0.7, 0.125]] },
  'intro:3:12': { steps: 4, vib: { rate: 5, cents: 14, delay: 0.2 } },
  'intro:4:10': { steps: 6, vib: { rate: 5, cents: 12, delay: 0.22 } },
  'intro:5:10': { steps: 6, vib: { rate: 5, cents: 12, delay: 0.22 } },
  'intro:7:4': { steps: 8, vib: { rate: 4.5, cents: 10, delay: 0.3 }, duty: [[0, 0.5], [0.5, 0.25]] },
  'loop:0:10': { steps: 6, vib: { rate: 5, cents: 12, delay: 0.22 } },
  'loop:1:10': { steps: 6, vib: { rate: 5, cents: 10, delay: 0.25 } },
  'loop:3:8': { steps: 4, from: G5, vib: { rate: 5, cents: 14, delay: 0.2 } },
  'loop:4:10': { steps: 6, vib: { rate: 5, cents: 12, delay: 0.22 } },
  'loop:5:13': { steps: 3 },
  'loop:8:10': { steps: 6, vib: { rate: 5, cents: 12, delay: 0.22 }, duty: [[0, 0.5], [0.45, 0.25]] },
  'loop:9:10': {
    steps: 6,
    vib: { rate: 5.5, cents: 16, delay: 0.2 },
    duty: [[0, 0.5], [0.4, 0.25], [0.72, 0.5]],
  },
  'loop:11:4': { steps: 4, vib: { rate: 5, cents: 14, delay: 0.2 } },
  'loop:12:10': { steps: 6, vib: { rate: 5, cents: 12, delay: 0.22 } },
  'loop:13:10': { steps: 6, vib: { rate: 5, cents: 10, delay: 0.25 } },
  'loop:25:0': {
    steps: 16,
    vib: { rate: 6, cents: 26, delay: 0.18 },
    duty: [[0, 0.125], [0.3, 0.25], [0.62, 0.5]],
  },
  'loop:27:0': { steps: 4, from: B5, vib: { rate: 5, cents: 16, delay: 0.22 } },
}

/**
 * Groove. Per-step time offsets in fractions of a sixteenth. The B section's
 * melody is written 3+1 inside each beat — a tracker shuffle — and this pulls
 * its short note back from 0.75 toward a triplet's 0.667, which is the
 * difference between mechanical and swaggering.
 */
const TITLE_STRAIGHT_GROOVE = new Array<number>(TITLE_STEPS_PER_BAR).fill(0)
const TITLE_SHUFFLE_GROOVE = TITLE_STRAIGHT_GROOVE.map((_, step) => (step % 4 === 3 ? -0.28 : 0))

/**
 * Continuous dynamics as [bar, level] breakpoints, smoothstep-interpolated.
 * The loop curve's last breakpoint is DEFINED equal to its first, so the seam
 * is level-continuous rather than merely close, and the intro curve ends on
 * that same value. The one steep move is 22.9 -> 23.04, the composed drop into
 * the bridge trap.
 */
const TITLE_INTRO_DYNAMICS: readonly (readonly [number, number])[] = [
  [0, 1], [0.9, 0.5], [2, 0.66], [3, 0.88], [4, 0.86],
  [5, 0.92], [6, 0.98], [7, 1.04], [8, 1.02],
]

const TITLE_LOOP_DYNAMICS: readonly (readonly [number, number])[] = [
  [0, 1.02], [3, 1.05], [7, 1.02], [8, 1], [11, 1.06], [15, 1.09],
  [16, 1], [19, 1.04], [20, 1.1], [22, 1.06],
  [22.9, 1.04], [23.04, 0.5], [24, 0.7], [25, 0.86], [26, 1.04],
  // The chromatic rip crescendos into the join — the loudest sixteenth in the
  // loop is its last — and the curve returns to its opening value at bar 28,
  // which IS bar 0. The rise earns the return; the shared endpoint keeps the
  // join level-continuous.
  [27, 1.12], [27.6, 1.15], [27.99, 1.24], [28, 1.02],
]

/**
 * A 128-step accent map rather than one 16-step bar repeated, so articulation
 * itself has a phrase rhythm: bars 4 and 8 of the cycle lift.
 */
const TITLE_ACCENTS = phrase<number>(
  [1.16, 0.92, 0.97, 1, 1.08, 0.92, 1, 1.05, 1.12, 0.93, 1, 0.97, 1.06, 0.93, 1, 1.04],
  [1.1, 0.94, 1.02, 0.96, 1.04, 0.95, 1.06, 0.98, 1.14, 0.94, 0.99, 1.03, 1.02, 0.96, 1.05, 1],
  [1.16, 0.92, 0.97, 1, 1.08, 0.92, 1, 1.05, 1.12, 0.93, 1, 0.97, 1.06, 0.93, 1, 1.04],
  [1.18, 0.95, 1, 1.06, 1.12, 0.96, 1.04, 1.1, 1.16, 0.98, 1.06, 1.1, 1.18, 1.06, 1.14, 1.22],
  [1.06, 0.9, 0.94, 1.02, 1, 0.9, 0.96, 1.04, 1.04, 0.91, 0.95, 1.02, 1, 0.92, 0.98, 1.08],
  [1.1, 0.94, 1.02, 0.96, 1.04, 0.95, 1.06, 0.98, 1.14, 0.94, 0.99, 1.03, 1.02, 0.96, 1.05, 1],
  [1.16, 0.92, 0.97, 1, 1.08, 0.92, 1, 1.05, 1.12, 0.93, 1, 0.97, 1.06, 0.93, 1, 1.04],
  [1.18, 0.95, 1, 1.06, 1.12, 0.96, 1.04, 1.1, 1.16, 0.98, 1.06, 1.1, 1.18, 1.06, 1.14, 1.22],
)

/**
 * Noise voices. `period` is a real 2A03 noise timer period: the LFSR clock is
 * 1789773 / period, so 4068 rumbles and 32 hisses. Timbre comes from the
 * period alone, which is why none of this needs a filter. `rateFrom` sweeps
 * playback rate — what a program does when it rewrites the period register
 * partway through a hit.
 */
const TITLE_DRUMS: Record<
  Exclude<TitleDrumStep, null>,
  { period: number; volume: number; decay: number; rateFrom: number }
> = {
  K: { period: 4068, volume: 0.029, decay: 0.125, rateFrom: 1.95 },
  k: { period: 4068, volume: 0.019, decay: 0.095, rateFrom: 1.7 },
  S: { period: 254, volume: 0.021, decay: 0.105, rateFrom: 1.16 },
  s: { period: 254, volume: 0.012, decay: 0.07, rateFrom: 1.1 },
  H: { period: 128, volume: 0.0105, decay: 0.142, rateFrom: 1 },
  h: { period: 128, volume: 0.0068, decay: 0.026, rateFrom: 1 },
  T: { period: 2034, volume: 0.0235, decay: 0.155, rateFrom: 1.55 },
  t: { period: 762, volume: 0.0165, decay: 0.115, rateFrom: 1.45 },
  C: { period: 64, volume: 0.0175, decay: 0.135, rateFrom: 1.05 },
  G: { period: 32, volume: 0.026, decay: 1.15, rateFrom: 1.9 },
  R: { period: 380, volume: 0.0115, decay: 0.05, rateFrom: 1 },
  1: { period: 762, volume: 0.0105, decay: 0.075, rateFrom: 1 },
  2: { period: 508, volume: 0.0125, decay: 0.075, rateFrom: 1 },
  3: { period: 254, volume: 0.0145, decay: 0.075, rateFrom: 1 },
  4: { period: 128, volume: 0.0165, decay: 0.075, rateFrom: 1 },
}

// ---------------------------------------------------------------------------
// Planning. Pure — no AudioContext — so the whole piece can be audited.
// ---------------------------------------------------------------------------

function titleSmoothstep(t: number): number {
  return t * t * (3 - 2 * t)
}

function titleInterpolate(curve: readonly (readonly [number, number])[], bar: number): number {
  if (bar <= curve[0][0]) return curve[0][1]
  for (let index = 1; index < curve.length; index += 1) {
    const [barB, levelB] = curve[index]
    if (bar <= barB) {
      const [barA, levelA] = curve[index - 1]
      const span = barB - barA
      const t = span <= 0 ? 1 : titleSmoothstep((bar - barA) / span)
      return levelA + (levelB - levelA) * t
    }
  }
  return curve[curve.length - 1][1]
}

function titleBlockFor(part: 'intro' | 'loop', bar: number): TitleBlockShape {
  let found: TitleBlockShape | null = null
  for (const block of TITLE_BLOCKS) {
    if (block.part === part && bar >= block.startBar) found = block
  }
  return found ?? TITLE_BLOCKS[0]
}

function titleGrooveFor(part: 'intro' | 'loop', bar: number): readonly number[] {
  return part === 'loop' && bar >= 16 && bar < 22 ? TITLE_SHUFFLE_GROOVE : TITLE_STRAIGHT_GROOVE
}

function titleStepTime(part: 'intro' | 'loop', step: number, stepSeconds: number): number {
  const bar = Math.floor(step / TITLE_STEPS_PER_BAR)
  return (step + titleGrooveFor(part, bar)[step % TITLE_STEPS_PER_BAR]) * stepSeconds
}

function titleOnsets<T>(steps: readonly (T | null)[]): number[] {
  const out: number[] = []
  for (let step = 0; step < steps.length; step += 1) {
    if (steps[step] !== null && steps[step] !== undefined) out.push(step)
  }
  return out
}

function titleLeadEvents(
  steps: readonly NoteStep[],
  part: 'intro' | 'loop',
  stepSeconds: number,
  partSeconds: number,
  dynamics: readonly (readonly [number, number])[],
): CueEvent[] {
  const onsets = titleOnsets(steps)
  const events: CueEvent[] = []
  for (let index = 0; index < onsets.length; index += 1) {
    const step = onsets[index]
    const bar = Math.floor(step / TITLE_STEPS_PER_BAR)
    const block = titleBlockFor(part, bar)
    const at = titleStepTime(part, step, stepSeconds)
    const isLast = index + 1 >= onsets.length
    const until = (isLast ? partSeconds : titleStepTime(part, onsets[index + 1], stepSeconds)) - at
    const hold = TITLE_HOLDS[`${part}:${bar}:${step % TITLE_STEPS_PER_BAR}`]
    events.push({
      channel: 'pulse1',
      kind: 'note',
      time: at,
      duration: isLast
        ? partSeconds - at
        : Math.max(
          0.02,
          Math.min((hold ? hold.steps : block.lead.maxGate) * stepSeconds, until - TITLE_VOICE_GAP),
        ),
      frequency: steps[step] as number,
      duty: block.lead.duty,
      dutyEnvelope: hold?.duty ?? null,
      vibrato: hold?.vib ?? null,
      portamentoFrom: hold?.from ?? null,
      attack: block.lead.attack,
      sustain: hold ? Math.max(block.lead.sustain, 0.74) : block.lead.sustain,
      gain: block.lead.volume
        * TITLE_ACCENTS[step % TITLE_ACCENTS.length]
        * titleInterpolate(dynamics, bar + (step % TITLE_STEPS_PER_BAR) / TITLE_STEPS_PER_BAR),
    })
  }
  return events
}

function titlePulse2Events(
  written: readonly NoteStep[],
  leadSteps: readonly NoteStep[],
  arps: readonly TitleArpBar[],
  roles: readonly TitlePulse2Role[],
  part: 'intro' | 'loop',
  stepSeconds: number,
  partSeconds: number,
  dynamics: readonly (readonly [number, number])[],
): CueEvent[] {
  /** The gate a written note wants; an arpeggio-chord's is its own duration. */
  interface Draft { event: CueEvent; wantedGate: number }
  const raw: Draft[] = []

  for (let bar = 0; bar < roles.length; bar += 1) {
    const role = roles[bar]
    const block = titleBlockFor(part, bar)
    const base = bar * TITLE_STEPS_PER_BAR

    if (role === 'arp') {
      const chords = arps[bar]
      if (!chords) continue
      for (let half = 0; half < 2; half += 1) {
        const step = base + half * (TITLE_STEPS_PER_BAR / 2)
        const halfBar = (TITLE_STEPS_PER_BAR / 2) * stepSeconds
        raw.push({
          wantedGate: halfBar,
          event: {
            channel: 'pulse2',
            kind: 'arp',
            time: titleStepTime(part, step, stepSeconds),
            duration: halfBar,
            chord: chords[half],
            frequency: chords[half][0],
            duty: block.p2.duty,
            attack: 0.008,
            sustain: 0.9,
            gain: block.p2.volume * titleInterpolate(dynamics, bar + half / 2),
          },
        })
      }
      continue
    }

    if (role === 'silent') continue

    const source = role === 'echo' ? leadSteps : written
    const shift = role === 'echo' ? 1 : 0
    for (let local = 0; local < TITLE_STEPS_PER_BAR; local += 1) {
      const from = base + local - shift
      if (from < 0 || from >= source.length) continue
      const pitch = source[from]
      if (pitch === null || pitch === undefined) continue
      raw.push({
        wantedGate: block.p2.maxGate * stepSeconds,
        event: {
          channel: 'pulse2',
          kind: 'note',
          time: titleStepTime(part, base + local, stepSeconds),
          duration: 0,
          frequency: pitch,
          duty: block.p2.duty,
          attack: block.p2.attack,
          sustain: block.p2.sustain,
          gain: block.p2.volume
            * TITLE_ACCENTS[(base + local) % TITLE_ACCENTS.length]
            * titleInterpolate(dynamics, bar + local / TITLE_STEPS_PER_BAR)
            * (role === 'echo' ? 0.72 : 1),
        },
      })
    }
  }

  raw.sort((a, b) => a.event.time - b.event.time)

  // One pass to enforce monophony after the merge, because pulse 2 swaps
  // between arpeggio-chords, echoes and written melody bar by bar.
  const out: CueEvent[] = []
  for (let index = 0; index < raw.length; index += 1) {
    const { event, wantedGate } = raw[index]
    const isLast = index + 1 >= raw.length
    const until = (isLast ? partSeconds : raw[index + 1].event.time) - event.time
    const duration = isLast
      ? partSeconds - event.time
      : Math.max(0.02, Math.min(wantedGate, until - TITLE_VOICE_GAP))
    if (duration <= 0.004) continue
    out.push({ ...event, duration })
  }
  return out
}

function titleTriangleEvents(
  steps: readonly NoteStep[],
  part: 'intro' | 'loop',
  stepSeconds: number,
  partSeconds: number,
  dynamics: readonly (readonly [number, number])[],
): CueEvent[] {
  const onsets = titleOnsets(steps)
  const events: CueEvent[] = []
  for (let index = 0; index < onsets.length; index += 1) {
    const step = onsets[index]
    const bar = Math.floor(step / TITLE_STEPS_PER_BAR)
    const block = titleBlockFor(part, bar)
    const at = titleStepTime(part, step, stepSeconds)
    const isLast = index + 1 >= onsets.length
    const until = (isLast ? partSeconds : titleStepTime(part, onsets[index + 1], stepSeconds)) - at
    events.push({
      channel: 'triangle',
      kind: 'note',
      time: at,
      duration: isLast
        ? partSeconds - at
        : Math.max(0.02, Math.min(block.tri.maxGate * stepSeconds, until - TITLE_VOICE_GAP)),
      frequency: steps[step] as number,
      attack: block.tri.attack,
      sustain: block.tri.sustain,
      // The bass follows the dynamics only partly. It is the floor; if it
      // ebbed as much as the melody the bridge would lose its footing.
      gain: block.tri.volume * (0.86 + 0.14 * titleInterpolate(
        dynamics,
        bar + (step % TITLE_STEPS_PER_BAR) / TITLE_STEPS_PER_BAR,
      )),
    })
  }
  return events
}

function titleNoiseEvents(
  steps: readonly TitleDrumStep[],
  part: 'intro' | 'loop',
  stepSeconds: number,
  partSeconds: number,
  dynamics: readonly (readonly [number, number])[],
): CueEvent[] {
  const onsets = titleOnsets(steps)
  const events: CueEvent[] = []
  for (let index = 0; index < onsets.length; index += 1) {
    const step = onsets[index]
    const drum = TITLE_DRUMS[steps[step] as Exclude<TitleDrumStep, null>]
    if (!drum) continue
    const bar = Math.floor(step / TITLE_STEPS_PER_BAR)
    const at = titleStepTime(part, step, stepSeconds)
    const isLast = index + 1 >= onsets.length
    const until = (isLast ? partSeconds : titleStepTime(part, onsets[index + 1], stepSeconds)) - at
    events.push({
      channel: 'noise',
      kind: 'noise',
      time: at,
      duration: isLast
        ? partSeconds - at
        : Math.max(0.015, Math.min(drum.decay, until - TITLE_VOICE_GAP)),
      period: drum.period,
      rateFrom: drum.rateFrom,
      gain: drum.volume * Math.min(
        1.14,
        titleInterpolate(dynamics, bar + (step % TITLE_STEPS_PER_BAR) / TITLE_STEPS_PER_BAR),
      ),
    })
  }
  return events
}

let titlePlan: TitleThemePlan | null = null

/** The whole piece, planned once and reused by every later start. */
function titleThemePlan(): TitleThemePlan {
  if (titlePlan) return titlePlan
  const stepSeconds = 60 / TITLE_BPM / 4
  const barSeconds = stepSeconds * TITLE_STEPS_PER_BAR
  const introDuration = TITLE_INTRO_BARS * barSeconds
  const loopDuration = TITLE_LOOP_BARS * barSeconds
  const byTime = (a: CueEvent, b: CueEvent): number =>
    a.time - b.time || a.channel.localeCompare(b.channel)
  titlePlan = {
    stepSeconds,
    introDuration,
    loopDuration,
    introEvents: [
      ...titleLeadEvents(TITLE_INTRO_PULSE1, 'intro', stepSeconds, introDuration, TITLE_INTRO_DYNAMICS),
      ...titlePulse2Events(
        TITLE_INTRO_PULSE2, TITLE_INTRO_PULSE1, TITLE_INTRO_ARP, TITLE_INTRO_P2,
        'intro', stepSeconds, introDuration, TITLE_INTRO_DYNAMICS,
      ),
      ...titleTriangleEvents(TITLE_INTRO_TRIANGLE, 'intro', stepSeconds, introDuration, TITLE_INTRO_DYNAMICS),
      ...titleNoiseEvents(TITLE_INTRO_NOISE, 'intro', stepSeconds, introDuration, TITLE_INTRO_DYNAMICS),
    ].sort(byTime),
    loopEvents: [
      ...titleLeadEvents(TITLE_LOOP_PULSE1, 'loop', stepSeconds, loopDuration, TITLE_LOOP_DYNAMICS),
      ...titlePulse2Events(
        TITLE_LOOP_PULSE2, TITLE_LOOP_PULSE1, TITLE_LOOP_ARP, TITLE_LOOP_P2,
        'loop', stepSeconds, loopDuration, TITLE_LOOP_DYNAMICS,
      ),
      ...titleTriangleEvents(TITLE_LOOP_TRIANGLE, 'loop', stepSeconds, loopDuration, TITLE_LOOP_DYNAMICS),
      ...titleNoiseEvents(TITLE_LOOP_NOISE, 'loop', stepSeconds, loopDuration, TITLE_LOOP_DYNAMICS),
    ].sort(byTime),
  }
  return titlePlan
}

// ---------------------------------------------------------------------------
// THE CUE RUNTIME. One scheduler, shared by the title score and the six round
// cues: an event list with per-note durations, duty envelopes, vibrato,
// portamento, arpeggio chords and LFSR noise, laid down intro-once then
// body-forever. Everything a cue needs is in its plan, so the runtime never
// asks which piece is playing.
// ---------------------------------------------------------------------------

/** All the runtime wants from a score: two flat event lists and their lengths. */
interface CuePlan {
  readonly introDuration: number
  readonly loopDuration: number
  readonly introEvents: readonly CueEvent[]
  readonly loopEvents: readonly CueEvent[]
}

interface CueRun {
  token: number
  context: AudioContext
  output: GainNode
  /** Unmuted output level for this cue. The title score and the rounds differ. */
  readonly volume: number
  plan: CuePlan
  sources: Set<AudioScheduledSourceNode>
  scheduler: number
  /** Context time of the first event of the intro. */
  originTime: number
  phase: 'intro' | 'loop'
  /** Index of the next event to schedule within the current part. */
  cursor: number
  /** How many times the loop body has already been laid down. */
  pass: number
  stopped: boolean
  /** Called once a through-composed cue has played out. Looping cues omit it. */
  onEnded?: () => void
}

/**
 * The 2A03 noise generator: a 15-bit shift register fed by bit 0 XOR bit 1,
 * clocked at 1789773 / period and held for the samples between. All timbre
 * comes from the period, which is why the noise channel needs no filter.
 */
const cueNoiseBuffers = new WeakMap<AudioContext, Map<number, AudioBuffer>>()

function lfsrNoiseBuffer(context: AudioContext, period: number): AudioBuffer {
  let byPeriod = cueNoiseBuffers.get(context)
  if (!byPeriod) {
    byPeriod = new Map()
    cueNoiseBuffers.set(context, byPeriod)
  }
  const existing = byPeriod.get(period)
  if (existing) return existing
  const hold = Math.max(1, Math.round(context.sampleRate / (1_789_773 / period)))
  const length = Math.max(1, Math.floor(context.sampleRate * 1.4))
  const buffer = context.createBuffer(1, length, context.sampleRate)
  const samples = buffer.getChannelData(0)
  let register = 1
  let value = 1
  let held = 0
  for (let index = 0; index < length; index += 1) {
    if (held === 0) {
      const feedback = (register & 1) ^ ((register >> 1) & 1)
      register = (register >> 1) | (feedback << 14)
      value = (register & 1) === 0 ? 1 : -1
      held = hold
    }
    held -= 1
    samples[index] = value
  }
  byPeriod.set(period, buffer)
  return buffer
}

function cueTrack(run: CueRun, node: AudioScheduledSourceNode, ...cleanup: AudioNode[]): void {
  run.sources.add(node)
  node.addEventListener('ended', () => {
    run.sources.delete(node)
    safeDisconnect(node)
    for (const extra of cleanup) safeDisconnect(extra)
  }, { once: true })
}

/**
 * A note stays audible until it is cut. An exponential run all the way to
 * -80 dB spends the back of every gate inaudible, which puts a hole in front
 * of every bar line; instead the tail decays to -16 dB of sustain and then
 * takes a 3.5 ms linear ramp to zero. That is also what makes the loop seam
 * continuous in level rather than merely in voice count.
 */
function cueEnvelope(gain: GainNode, event: CueEvent, at: number): void {
  const peak = Math.max(event.gain, 0.0002)
  const end = at + event.duration
  const attackAt = at + Math.min(event.attack as number, event.duration * 0.22)
  const decayAt = Math.min(
    at + event.duration * 0.72,
    Math.max(attackAt + 0.004, at + event.duration * 0.38),
  )
  const sustainLevel = Math.max(peak * (event.sustain as number), 0.0002)
  const cutAt = Math.max(decayAt + 0.002, end - CUE_RELEASE_SECONDS)
  gain.gain.setValueAtTime(0.0001, at)
  gain.gain.exponentialRampToValueAtTime(peak, attackAt)
  gain.gain.exponentialRampToValueAtTime(sustainLevel, decayAt)
  gain.gain.exponentialRampToValueAtTime(Math.max(sustainLevel * 0.16, 0.0002), cutAt)
  gain.gain.linearRampToValueAtTime(0.00001, end)
}

/** Vibrato and portamento are frequency-param writes — the NES pitch table. */
function cueApplyPitch(
  oscillator: OscillatorNode,
  event: CueEvent,
  at: number,
  segmentStart: number,
  segmentEnd: number,
): void {
  const base = event.frequency as number
  if (event.portamentoFrom && segmentStart === at) {
    oscillator.frequency.setValueAtTime(event.portamentoFrom, at)
    oscillator.frequency.exponentialRampToValueAtTime(
      base,
      at + Math.min(0.055, event.duration * 0.3),
    )
  } else {
    oscillator.frequency.setValueAtTime(base, segmentStart)
  }
  if (!event.vibrato) return
  const { rate, cents, delay } = event.vibrato
  const from = at + event.duration * delay
  for (let t = Math.max(segmentStart, from); t < segmentEnd; t += CUE_FRAME_SECONDS) {
    oscillator.frequency.setValueAtTime(base * (2 ** ((cents * Math.sin(Math.PI * 2 * rate * (t - from))) / 1200)), t)
  }
}

function scheduleCueNote(run: CueRun, event: CueEvent, at: number): void {
  const gain = run.context.createGain()
  // A GainNode is born at unity and its first automation point does not apply
  // retroactively, so a source starting a sample early would put a full-scale
  // click in the mix. Park the node at silence first.
  gain.gain.value = 0.0001
  cueEnvelope(gain, event, at)
  gain.connect(run.output)

  // A duty envelope becomes butt-spliced segments on this ONE gain node. The
  // segments tile the note exactly, so the channel still sounds a single
  // voice — which is what happens on hardware when a program rewrites the
  // duty bits partway through a note.
  const segments: readonly CueDutySegment[] = event.dutyEnvelope && event.dutyEnvelope.length > 1
    ? event.dutyEnvelope
    : [[0, event.duty ?? 0.5] as const]

  for (let index = 0; index < segments.length; index += 1) {
    const [fraction, duty] = segments[index]
    const nextFraction = index + 1 < segments.length ? segments[index + 1][0] : 1
    const segmentStart = at + event.duration * fraction
    const segmentEnd = at + event.duration * nextFraction
    const oscillator = run.context.createOscillator()
    if (event.channel === 'triangle') {
      oscillator.type = 'triangle'
    } else {
      oscillator.type = 'square'
      if (
        typeof run.context.createPeriodicWave === 'function'
        && typeof oscillator.setPeriodicWave === 'function'
      ) {
        oscillator.setPeriodicWave(pulseWave(run.context, duty))
      }
    }
    cueApplyPitch(oscillator, event, at, segmentStart, segmentEnd)
    oscillator.connect(gain)
    cueTrack(run, oscillator, ...(index + 1 === segments.length ? [gain] : []))
    oscillator.start(segmentStart)
    oscillator.stop(segmentEnd)
  }
}

/**
 * ARPEGGIO-AS-CHORD. One oscillator, one gain, and a frequency write every NES
 * frame. Three notes cycling at 60 Hz shimmer at 20 Hz and the ear fuses them
 * into a triad — harmony for no extra channel, which is the only reason pulse
 * 1 gets to sing over real chords in the A section.
 */
function scheduleCueArp(run: CueRun, event: CueEvent, at: number): void {
  const oscillator = run.context.createOscillator()
  const gain = run.context.createGain()
  gain.gain.value = 0.0001
  const chord = event.chord as readonly number[]
  oscillator.type = 'square'
  if (
    typeof run.context.createPeriodicWave === 'function'
    && typeof oscillator.setPeriodicWave === 'function'
  ) {
    oscillator.setPeriodicWave(pulseWave(run.context, event.duty ?? 0.25))
  }
  let index = 0
  for (let t = at; t < at + event.duration; t += CUE_FRAME_SECONDS) {
    oscillator.frequency.setValueAtTime(chord[index % chord.length], t)
    index += 1
  }
  cueEnvelope(gain, event, at)
  oscillator.connect(gain)
  gain.connect(run.output)
  cueTrack(run, oscillator, gain)
  oscillator.start(at)
  oscillator.stop(at + event.duration)
}

function scheduleCueNoise(run: CueRun, event: CueEvent, at: number): void {
  const source = run.context.createBufferSource()
  const gain = run.context.createGain()
  gain.gain.value = 0.0001
  source.buffer = lfsrNoiseBuffer(run.context, event.period as number)
  if (event.rateFrom !== 1) {
    source.playbackRate.setValueAtTime(event.rateFrom as number, at)
    source.playbackRate.exponentialRampToValueAtTime(1, at + event.duration)
  }
  gain.gain.setValueAtTime(Math.max(event.gain, 0.0002), at)
  gain.gain.exponentialRampToValueAtTime(0.0001, at + event.duration)
  source.connect(gain)
  gain.connect(run.output)
  cueTrack(run, source, gain)
  source.start(at)
  source.stop(at + event.duration)
}

function scheduleCueEvent(run: CueRun, event: CueEvent, at: number): void {
  if (event.kind === 'noise') scheduleCueNoise(run, event, at)
  else if (event.kind === 'arp') scheduleCueArp(run, event, at)
  else scheduleCueNote(run, event, at)
}

/**
 * Lay down every event that starts inside the lookahead window: the intro once
 * and then the loop body forever, each pass offset by one loop duration so the
 * seam is arithmetic rather than a re-entry.
 *
 * A plan with no loop events is through-composed and ENDS — that is the
 * credits cue. It runs its list once and then hands itself to the ending
 * callback rather than wrapping.
 *
 * A run is live if it is whichever of the three active-cue slots points at it.
 */
function fillCueSchedule(run: CueRun): void {
  if (
    run.stopped
    || (activeTitleTheme !== run && activeRoundTheme !== run && activeCreditsTheme !== run)
  ) {
    return
  }
  const now = run.context.currentTime
  const horizon = now + CUE_LOOKAHEAD_SECONDS
  const { plan } = run
  for (let guard = 0; guard < 8_000; guard += 1) {
    const list = run.phase === 'intro' ? plan.introEvents : plan.loopEvents
    if (run.cursor >= list.length) {
      if (list === plan.loopEvents && plan.loopEvents.length === 0) {
        if (now - run.originTime >= plan.introDuration) run.onEnded?.()
        return
      }
      if (run.phase === 'intro') run.phase = 'loop'
      else run.pass += 1
      run.cursor = 0
      continue
    }
    const base = run.phase === 'intro' ? 0 : plan.introDuration + run.pass * plan.loopDuration
    const event = list[run.cursor]
    const at = run.originTime + base + event.time
    if (at >= horizon) return
    // An event whose time has already passed is dropped rather than crammed in
    // late: a retrigger cluster would break the four-voice ceiling.
    if (at >= now) scheduleCueEvent(run, event, at)
    run.cursor += 1
  }
}

export function startOriginalTitleTheme(): boolean {
  if (
    (activeRoundTheme && !activeRoundTheme.stopped)
    || (activeTitleTheme && !activeTitleTheme.stopped)
  ) {
    return false
  }
  const context = audioContext()
  if (!context) return false
  // The credits cue is the one thing the title score outranks: closing the
  // roll lands back here, and this effect is what makes the title screen
  // audible again. Stopped only once the start is certain to succeed.
  stopOriginalCreditsTheme()
  const entersAt = context.currentTime + 0.08
  const output = context.createGain()
  output.gain.value = 0.0001
  output.gain.setValueAtTime(0.0001, context.currentTime)
  output.gain.setValueAtTime(0.0001, entersAt)
  output.gain.exponentialRampToValueAtTime(
    originalTitleThemeMuted ? 0.0001 : ORIGINAL_TITLE_THEME_VOLUME,
    entersAt + 0.14,
  )
  output.connect(context.destination)
  const run: CueRun = {
    token: ++nextThemeToken,
    context,
    output,
    volume: ORIGINAL_TITLE_THEME_VOLUME,
    plan: titleThemePlan(),
    sources: new Set(),
    scheduler: 0,
    // Every start begins a fresh run at the top of the intro.
    originTime: entersAt,
    phase: 'intro',
    cursor: 0,
    pass: 0,
    stopped: false,
  }
  activeTitleTheme = run
  fillCueSchedule(run)
  run.scheduler = window.setInterval(() => fillCueSchedule(run), CUE_SCHEDULER_MS)
  return true
}

export function stopOriginalTitleTheme(): boolean {
  const run = activeTitleTheme
  if (!run || run.stopped) return false
  activeTitleTheme = null
  run.stopped = true
  window.clearInterval(run.scheduler)
  const now = run.context.currentTime
  const stopAt = now + 0.12
  run.output.gain.cancelScheduledValues(now)
  run.output.gain.setValueAtTime(Math.max(run.output.gain.value, 0.0001), now)
  run.output.gain.exponentialRampToValueAtTime(0.0001, stopAt)
  for (const source of run.sources) {
    try {
      source.stop(stopAt)
    } catch {
      // A source may already have ended between iteration and stop.
    }
  }
  window.setTimeout(() => {
    for (const source of run.sources) safeDisconnect(source)
    run.sources.clear()
    safeDisconnect(run.output)
  }, 160)
  return true
}

export function setOriginalTitleThemeMuted(muted: boolean): void {
  originalTitleThemeMuted = muted
  const run = activeTitleTheme
  if (!run || run.stopped) return
  const now = run.context.currentTime
  run.output.gain.cancelScheduledValues(now)
  run.output.gain.setValueAtTime(Math.max(run.output.gain.value, 0.0001), now)
  run.output.gain.exponentialRampToValueAtTime(
    muted ? 0.0001 : ORIGINAL_TITLE_THEME_VOLUME,
    now + (muted ? 0.04 : 0.1),
  )
}

// ---------------------------------------------------------------------------
// THE ROUND CUES. Six underscores from round-score.ts, on the same runtime as
// the title score: a four-bar intro once, then a 32-bar body forever. They are
// written to be talked over — 76 to 92 BPM, sparse, and quiet enough at
// ROUND_THEME_VOLUME to sit under narration rather than compete with it.
// ---------------------------------------------------------------------------

/**
 * Start a round cue. The 0.08 s pre-roll is only there to clear the current
 * render quantum — every cue opens on a single triangle note, so there is
 * nothing to ease in beyond the 0.2 s ramp on the master.
 */
export function startOriginalRoundTheme(roundId: RoundId): boolean {
  stopOriginalTitleTheme()
  stopOriginalCreditsTheme()
  if (activeRoundTheme && !activeRoundTheme.stopped) return false
  const context = audioContext()
  if (!context) return false
  const entersAt = context.currentTime + 0.08
  const output = context.createGain()
  // Parked at silence: a GainNode is born at unity and its first automation
  // point does not apply retroactively.
  output.gain.value = 0.0001
  output.gain.setValueAtTime(0.0001, context.currentTime)
  output.gain.setValueAtTime(0.0001, entersAt)
  output.gain.exponentialRampToValueAtTime(
    originalRoundThemeMuted ? 0.0001 : ROUND_THEME_VOLUME,
    entersAt + 0.2,
  )
  output.connect(context.destination)
  const run: CueRun = {
    token: ++nextThemeToken,
    context,
    output,
    volume: ROUND_THEME_VOLUME,
    plan: roundCuePlan(roundId),
    sources: new Set(),
    scheduler: 0,
    originTime: entersAt,
    phase: 'intro',
    cursor: 0,
    pass: 0,
    stopped: false,
  }
  activeRoundTheme = run
  fillCueSchedule(run)
  run.scheduler = window.setInterval(() => fillCueSchedule(run), CUE_SCHEDULER_MS)
  return true
}

export function stopOriginalRoundTheme(): boolean {
  const run = activeRoundTheme
  if (!run || run.stopped) return false
  activeRoundTheme = null
  run.stopped = true
  window.clearInterval(run.scheduler)
  const now = run.context.currentTime
  const stopAt = now + 0.12
  run.output.gain.cancelScheduledValues(now)
  run.output.gain.setValueAtTime(Math.max(run.output.gain.value, 0.0001), now)
  run.output.gain.exponentialRampToValueAtTime(0.0001, stopAt)
  for (const source of run.sources) {
    try {
      source.stop(stopAt)
    } catch {
      // A source may already have ended between iteration and stop.
    }
  }
  window.setTimeout(() => {
    for (const source of run.sources) safeDisconnect(source)
    run.sources.clear()
    safeDisconnect(run.output)
  }, 160)
  return true
}

/**
 * Honoured at start time as well as mid-playback, so the flag survives a stop
 * and a later start — the same contract as the title theme's mute.
 */
export function setOriginalRoundThemeMuted(muted: boolean): void {
  originalRoundThemeMuted = muted
  const run = activeRoundTheme
  if (!run || run.stopped) return
  const now = run.context.currentTime
  run.output.gain.cancelScheduledValues(now)
  run.output.gain.setValueAtTime(Math.max(run.output.gain.value, 0.0001), now)
  run.output.gain.exponentialRampToValueAtTime(
    muted ? 0.0001 : run.volume,
    now + (muted ? 0.04 : 0.1),
  )
}

// ---------------------------------------------------------------------------
// THE CREDITS CUE. "The Long Walk Back" from credits-score.ts: 40 bars,
// through-composed, ends rather than loops.
//
// THE TRANSPORT POLICY, and why it is asymmetric. Priority runs round > title
// > credits, and only the credits cue is ever refused.
//
//   - A ROUND cue refuses it outright. The themes are start/stop with no seek,
//     so preempting a live bout means the round theme restarts from bar one
//     when the roll closes -- and startOriginalRoundTheme() returns false while
//     something else holds the transport, so a stop/restore could not even
//     confirm it worked. "The credits were quiet because a round was playing"
//     is a shrug; "the round theme died mid-proof" is a broken demo.
//
//   - The TITLE theme yields to it. That is not the same trade. The title
//     score is a stateless attract loop that is already restarted from bar one
//     every single time the app lands on the title screen, so there is no
//     position to lose and nothing to restore -- closing the roll returns to
//     the title screen, whose own effect starts it again. startOriginalRound-
//     Theme() has stopped it unconditionally since long before this cue
//     existed; the credits follow that precedent rather than inventing one.
//
// The whole policy lives here rather than in the component, so the boolean
// means exactly one thing at every call site: the credits cue is now playing.
// ---------------------------------------------------------------------------

/**
 * Start the credits cue. Returns false, and plays nothing at all, if a round
 * cue holds the transport or the credits cue is already running.
 */
export function startOriginalCreditsTheme(): boolean {
  if (activeRoundTheme && !activeRoundTheme.stopped) return false
  if (activeCreditsTheme && !activeCreditsTheme.stopped) return false
  const context = audioContext()
  if (!context) return false
  stopOriginalTitleTheme()
  const entersAt = context.currentTime + 0.08
  const output = context.createGain()
  // Parked at silence: a GainNode is born at unity and its first automation
  // point does not apply retroactively.
  output.gain.value = 0.0001
  output.gain.setValueAtTime(0.0001, context.currentTime)
  output.gain.setValueAtTime(0.0001, entersAt)
  output.gain.exponentialRampToValueAtTime(
    originalCreditsThemeMuted ? 0.0001 : CREDITS_THEME_VOLUME,
    entersAt + 0.14,
  )
  output.connect(context.destination)
  const run: CueRun = {
    token: ++nextThemeToken,
    context,
    output,
    volume: CREDITS_THEME_VOLUME,
    plan: creditsCuePlan(),
    sources: new Set(),
    scheduler: 0,
    originTime: entersAt,
    phase: 'intro',
    cursor: 0,
    pass: 0,
    stopped: false,
    // The piece ends. Tear the run down on the final chord's release so the
    // slot is free and a later roll starts clean, rather than leaving a
    // scheduler ticking over an empty list.
    onEnded: () => { stopOriginalCreditsTheme() },
  }
  activeCreditsTheme = run
  fillCueSchedule(run)
  run.scheduler = window.setInterval(() => fillCueSchedule(run), CUE_SCHEDULER_MS)
  return true
}

export function stopOriginalCreditsTheme(): boolean {
  const run = activeCreditsTheme
  if (!run || run.stopped) return false
  activeCreditsTheme = null
  run.stopped = true
  window.clearInterval(run.scheduler)
  const now = run.context.currentTime
  const stopAt = now + 0.12
  run.output.gain.cancelScheduledValues(now)
  run.output.gain.setValueAtTime(Math.max(run.output.gain.value, 0.0001), now)
  run.output.gain.exponentialRampToValueAtTime(0.0001, stopAt)
  for (const source of run.sources) {
    try {
      source.stop(stopAt)
    } catch {
      // A source may already have ended between iteration and stop.
    }
  }
  window.setTimeout(() => {
    for (const source of run.sources) safeDisconnect(source)
    run.sources.clear()
    safeDisconnect(run.output)
  }, 160)
  return true
}

/**
 * Honoured at start time as well as mid-playback, so the flag survives a stop
 * and a later start — the same contract as the title and round mutes.
 */
export function setOriginalCreditsThemeMuted(muted: boolean): void {
  originalCreditsThemeMuted = muted
  const run = activeCreditsTheme
  if (!run || run.stopped) return
  const now = run.context.currentTime
  run.output.gain.cancelScheduledValues(now)
  run.output.gain.setValueAtTime(Math.max(run.output.gain.value, 0.0001), now)
  run.output.gain.exponentialRampToValueAtTime(
    muted ? 0.0001 : run.volume,
    now + (muted ? 0.04 : 0.1),
  )
}

export function playOriginalBell(): void {
  const context = audioContext()
  if (!context) return
  const now = context.currentTime
  const gain = context.createGain()
  gain.gain.setValueAtTime(0.0001, now)
  gain.gain.exponentialRampToValueAtTime(0.18, now + 0.01)
  gain.gain.exponentialRampToValueAtTime(0.0001, now + 0.72)
  gain.connect(context.destination)
  ;[620, 930, 1240].forEach((frequency, index) => {
    const oscillator = context.createOscillator()
    oscillator.type = index === 0 ? 'triangle' : 'sine'
    oscillator.frequency.setValueAtTime(frequency, now)
    oscillator.frequency.exponentialRampToValueAtTime(frequency * 0.88, now + 0.65)
    oscillator.connect(gain)
    oscillator.start(now)
    oscillator.stop(now + 0.75)
  })
  window.setTimeout(() => safeDisconnect(gain), 900)
}

function playTone(frequency: number, duration: number, type: OscillatorType = 'square', volume = 0.045): void {
  const context = audioContext()
  if (!context) return
  const titleRun = activeTitleTheme
  if (titleRun && !titleRun.stopped && !originalTitleThemeMuted) {
    const duckAt = context.currentTime
    const recoverAt = duckAt + duration + 0.025
    titleRun.output.gain.cancelScheduledValues(duckAt)
    titleRun.output.gain.setValueAtTime(Math.max(titleRun.output.gain.value, 0.0001), duckAt)
    titleRun.output.gain.exponentialRampToValueAtTime(0.045, duckAt + 0.012)
    titleRun.output.gain.setValueAtTime(0.045, recoverAt)
    titleRun.output.gain.exponentialRampToValueAtTime(ORIGINAL_TITLE_THEME_VOLUME, recoverAt + 0.12)
  }
  const oscillator = context.createOscillator()
  const gain = context.createGain()
  const now = context.currentTime
  oscillator.type = type
  oscillator.frequency.setValueAtTime(frequency, now)
  gain.gain.setValueAtTime(volume, now)
  gain.gain.exponentialRampToValueAtTime(0.0001, now + duration)
  oscillator.connect(gain)
  gain.connect(context.destination)
  oscillator.start(now)
  oscillator.stop(now + duration)
  oscillator.addEventListener('ended', () => {
    safeDisconnect(oscillator)
    safeDisconnect(gain)
  }, { once: true })
}

export function playCursor(): void {
  playTone(420, 0.045)
}

export function playConfirm(): void {
  playTone(660, 0.08)
  window.setTimeout(() => playTone(880, 0.1), 65)
}

export function playStart(delayMs = 0): void {
  if (delayMs > 0) window.setTimeout(() => playTone(330, 0.09), delayMs)
  else playTone(330, 0.09)
  window.setTimeout(() => playTone(495, 0.09), delayMs + 85)
  window.setTimeout(() => playTone(660, 0.16), delayMs + 170)
}

declare global {
  interface Window {
    webkitAudioContext?: typeof AudioContext
  }
}
