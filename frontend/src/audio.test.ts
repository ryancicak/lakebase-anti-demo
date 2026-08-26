import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

class FakeAudioParam {
  value = 1
  setValueAtTime = vi.fn((value: number) => { this.value = value })
  exponentialRampToValueAtTime = vi.fn((value: number) => { this.value = value })
  linearRampToValueAtTime = vi.fn((value: number) => { this.value = value })
  cancelScheduledValues = vi.fn()
}

class FakeSource {
  static all: FakeSource[] = []
  type: OscillatorType = 'sine'
  frequency = new FakeAudioParam()
  playbackRate = new FakeAudioParam()
  buffer: AudioBuffer | null = null
  connect = vi.fn()
  disconnect = vi.fn()
  start = vi.fn()
  stop = vi.fn()
  setPeriodicWave = vi.fn()
  addEventListener = vi.fn()

  constructor() {
    FakeSource.all.push(this)
  }
}

class FakeGain {
  gain = new FakeAudioParam()
  connect = vi.fn()
  disconnect = vi.fn()
}

class FakeAudioContext {
  static instances: FakeAudioContext[] = []
  readonly destination = {}
  readonly sampleRate = 1_000
  state: AudioContextState = 'running'
  resume = vi.fn(() => Promise.resolve())
  createGain = vi.fn(() => new FakeGain())
  createOscillator = vi.fn(() => new FakeSource())
  createPeriodicWave = vi.fn(() => ({} as PeriodicWave))
  createBufferSource = vi.fn(() => new FakeSource())
  createBuffer = vi.fn((_channels: number, length: number) => {
    const data = new Float32Array(length)
    return { getChannelData: () => data } as unknown as AudioBuffer
  })

  get currentTime() {
    return Date.now() / 1_000
  }

  constructor() {
    FakeAudioContext.instances.push(this)
  }
}

describe('original round theme', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(0)
    vi.resetModules()
    FakeAudioContext.instances = []
    FakeSource.all = []
    vi.stubGlobal('AudioContext', FakeAudioContext)
  })

  afterEach(() => {
    vi.useRealTimers()
    vi.unstubAllGlobals()
  })

  it('shares one lazy context, guards duplicate starts, and stages the complete wake-up arrangement', async () => {
    const audio = await import('./audio')

    audio.playOriginalBell()
    expect(audio.startOriginalRoundTheme('wake_idle_app')).toBe(true)
    expect(audio.startOriginalRoundTheme('wake_idle_app')).toBe(false)
    // 'Idle' opens on triangle and a soft floor tom alone — the first bars are
    // deliberately a rest for whoever is talking. Pulse 1 does not answer until
    // 7.9 s, so the window has to reach bar three to see the whole texture.
    vi.advanceTimersByTime(9_000)

    expect(FakeAudioContext.instances).toHaveLength(1)
    expect(FakeSource.all.some((source) => source.type === 'square')).toBe(true)
    expect(FakeSource.all.some((source) => source.type === 'triangle')).toBe(true)
    expect(FakeAudioContext.instances[0].createBufferSource).toHaveBeenCalled()
    expect(FakeAudioContext.instances[0].createPeriodicWave).toHaveBeenCalledTimes(1)
    expect(FakeSource.all.some((source) => source.setPeriodicWave.mock.calls.length > 0)).toBe(true)
    audio.stopOriginalRoundTheme()
  })

  it('fades and stops tracked sources, then safely starts a later round', async () => {
    const audio = await import('./audio')

    expect(audio.startOriginalRoundTheme('wake_idle_app')).toBe(true)
    vi.advanceTimersByTime(700)
    const firstRunSources = [...FakeSource.all]
    expect(firstRunSources.length).toBeGreaterThan(0)

    expect(audio.stopOriginalRoundTheme()).toBe(true)
    expect(audio.stopOriginalRoundTheme()).toBe(false)
    expect(firstRunSources.some((source) => source.stop.mock.calls.length > 1)).toBe(true)
    vi.advanceTimersByTime(150)

    expect(audio.startOriginalRoundTheme('survive_connection_spike')).toBe(true)
    vi.advanceTimersByTime(700)
    expect(FakeAudioContext.instances).toHaveLength(1)
    expect(FakeSource.all.length).toBeGreaterThan(firstRunSources.length)
    audio.stopOriginalRoundTheme()
  })

  it('keeps the attract-mode title score mutually exclusive with round music', async () => {
    const audio = await import('./audio')

    expect(audio.startOriginalTitleTheme()).toBe(true)
    expect(audio.startOriginalTitleTheme()).toBe(false)
    vi.advanceTimersByTime(600)
    const titleSources = [...FakeSource.all]
    expect(titleSources.length).toBeGreaterThan(0)

    // Starting a bout owns the mix: title sources fade/stop before the delayed
    // round entrance, and the title cannot be restarted over an active round.
    expect(audio.startOriginalRoundTheme('wake_idle_app')).toBe(true)
    expect(audio.stopOriginalTitleTheme()).toBe(false)
    expect(titleSources.some((source) => source.stop.mock.calls.length > 1)).toBe(true)
    expect(audio.startOriginalTitleTheme()).toBe(false)

    audio.stopOriginalRoundTheme()
    vi.advanceTimersByTime(150)
    expect(audio.startOriginalTitleTheme()).toBe(true)
    audio.setOriginalTitleThemeMuted(true)
    audio.setOriginalTitleThemeMuted(false)
    audio.stopOriginalTitleTheme()
  })

  it('keeps the title score inside the four-voice 2A03 budget, intro through loop', async () => {
    const audio = await import('./audio')

    expect(audio.startOriginalTitleTheme()).toBe(true)
    // Past the bell, the fanfare and the hook, and over the intro handoff.
    vi.advanceTimersByTime(20_000)

    const spans = FakeSource.all
      .filter((source) => source.start.mock.calls.length > 0 && source.stop.mock.calls.length > 0)
      .map((source): [number, number] => [
        source.start.mock.calls[0][0] as number,
        source.stop.mock.calls[0][0] as number,
      ])
    // The eight-bar intro alone plans 152 events, and the window reaches into
    // the loop body, so this is comfortably past the handoff.
    expect(spans.length).toBeGreaterThan(180)

    // Sweep the onsets: half-open intervals, so butt-spliced duty segments and
    // a retriggered channel both count as one voice, exactly as the hardware
    // would sound them.
    let peak = 0
    for (const [onset] of spans) {
      const sounding = spans.filter(([from, until]) => from <= onset && until > onset).length
      if (sounding > peak) peak = sounding
    }
    expect(peak).toBe(4)

    // The bell voices all four channels at once, so the first bar alone proves
    // the ceiling is reached and never exceeded.
    audio.stopOriginalTitleTheme()
  })

  it('keeps every round cue inside the four-voice 2A03 budget', async () => {
    const audio = await import('./audio')
    const roundIds = [
      'wake_idle_app',
      'make_schema_change_safely',
      'recover_deleted_order',
      'put_model_score_in_app',
      'survive_connection_spike',
      'analyze_live_orders_without_slowing_checkout',
    ] as const

    for (const roundId of roundIds) {
      const firstSource = FakeSource.all.length
      expect(audio.startOriginalRoundTheme(roundId)).toBe(true)
      // Through the four-bar intro and into the loop body, so the handoff and
      // the densest development bars are both inside the window.
      vi.advanceTimersByTime(30_000)

      const spans = FakeSource.all
        .slice(firstSource)
        .filter((source) => source.start.mock.calls.length > 0 && source.stop.mock.calls.length > 0)
        .map((source): [number, number] => [
          source.start.mock.calls[0][0] as number,
          source.stop.mock.calls[0][0] as number,
        ])
      // A floor only so the ceiling below cannot pass vacuously. It is this low
      // because 'Idle' really is that sparse — under one voice a second.
      expect(spans.length).toBeGreaterThan(10)

      // Half-open intervals, so a retriggered channel and butt-spliced duty
      // segments each count as one voice, exactly as the hardware sounds them.
      let peak = 0
      for (const [onset] of spans) {
        const sounding = spans.filter(([from, until]) => from <= onset && until > onset).length
        if (sounding > peak) peak = sounding
      }
      expect(peak).toBeLessThanOrEqual(4)

      audio.stopOriginalRoundTheme()
      vi.advanceTimersByTime(200)
    }
  })

  it('dispatches a distinct arrangement for every round', async () => {
    const audio = await import('./audio')
    const roundIds = [
      'wake_idle_app',
      'make_schema_change_safely',
      'recover_deleted_order',
      'put_model_score_in_app',
      'survive_connection_spike',
      'analyze_live_orders_without_slowing_checkout',
    ] as const
    const signatures: string[] = []

    for (const roundId of roundIds) {
      const firstSource = FakeSource.all.length
      expect(audio.startOriginalRoundTheme(roundId)).toBe(true)
      vi.advanceTimersByTime(6_000)
      // Pitch and onset for every voice of the opening phrase, relative to the
      // cue's first note. The six cues are sparse and share a motif, so one
      // note no longer tells them apart — the arrangement does.
      const voices = FakeSource.all.slice(firstSource)
      expect(voices.length).toBeGreaterThan(0)
      const origin = voices[0].start.mock.calls[0][0] as number
      signatures.push(voices.map((voice) => {
        const pitch = voice.frequency.setValueAtTime.mock.calls[0]?.[0] ?? 0
        const onset = (voice.start.mock.calls[0][0] as number) - origin
        return `${voice.type}:${Math.round(Number(pitch))}@${onset.toFixed(3)}`
      }).join('|'))
      audio.stopOriginalRoundTheme()
      vi.advanceTimersByTime(150)
    }

    expect(new Set(signatures)).toHaveLength(roundIds.length)
  })
})

describe('original credits theme', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(0)
    vi.resetModules()
    FakeAudioContext.instances = []
    FakeSource.all = []
    vi.stubGlobal('AudioContext', FakeAudioContext)
  })

  afterEach(() => {
    vi.useRealTimers()
    vi.unstubAllGlobals()
  })

  it('declines to a live bout and plays nothing at all', async () => {
    const audio = await import('./audio')

    expect(audio.startOriginalRoundTheme('survive_connection_spike')).toBe(true)
    vi.advanceTimersByTime(4_000)
    const duringBout = FakeSource.all.length

    // The refusal is the whole point: a round cue cannot be seeked, so taking
    // the transport from one would restart it from bar one when the roll closes.
    expect(audio.startOriginalCreditsTheme()).toBe(false)
    expect(audio.stopOriginalCreditsTheme()).toBe(false)
    vi.advanceTimersByTime(8_000)

    // The round is still running and nothing of its own was stopped.
    expect(FakeSource.all.length).toBeGreaterThan(duringBout)
    expect(FakeSource.all.slice(0, duringBout).every(
      (source) => source.stop.mock.calls.length <= 1,
    )).toBe(true)
    audio.stopOriginalRoundTheme()
  })

  it('takes the transport from the title score, and hands it straight back', async () => {
    const audio = await import('./audio')

    expect(audio.startOriginalTitleTheme()).toBe(true)
    vi.advanceTimersByTime(1_500)
    const titleSources = [...FakeSource.all]
    expect(titleSources.length).toBeGreaterThan(0)

    // The attract loop is stateless and is restarted from bar one every time
    // the app lands on the title screen, so there is no position to lose.
    expect(audio.startOriginalCreditsTheme()).toBe(true)
    expect(audio.stopOriginalTitleTheme()).toBe(false)
    expect(titleSources.some((source) => source.stop.mock.calls.length > 1)).toBe(true)
    expect(audio.startOriginalCreditsTheme()).toBe(false)

    // Closing the roll lands back on the title screen, whose own effect starts
    // the title score again — which takes the transport back.
    vi.advanceTimersByTime(1_000)
    const creditsSources = [...FakeSource.all]
    expect(audio.startOriginalTitleTheme()).toBe(true)
    expect(creditsSources.slice(titleSources.length).some(
      (source) => source.stop.mock.calls.length > 1,
    )).toBe(true)
    audio.stopOriginalTitleTheme()
  })

  it('yields to a round cue rather than blocking one', async () => {
    const audio = await import('./audio')

    expect(audio.startOriginalCreditsTheme()).toBe(true)
    vi.advanceTimersByTime(1_500)
    expect(audio.startOriginalRoundTheme('wake_idle_app')).toBe(true)
    expect(audio.stopOriginalCreditsTheme()).toBe(false)
    audio.stopOriginalRoundTheme()
  })

  it('mutes and unmutes a running cue without stopping or restarting it', async () => {
    const audio = await import('./audio')

    expect(audio.startOriginalCreditsTheme()).toBe(true)
    vi.advanceTimersByTime(3_000)
    const beforeMute = [...FakeSource.all]
    const gain = FakeAudioContext.instances[0].createGain.mock.results[0].value as {
      gain: { value: number }
    }

    audio.setOriginalCreditsThemeMuted(true)
    expect(gain.gain.value).toBeCloseTo(0.0001, 6)
    vi.advanceTimersByTime(3_000)
    audio.setOriginalCreditsThemeMuted(false)
    expect(gain.gain.value).toBeCloseTo(0.14, 6)

    // Nothing already sounding was cut, and the cue kept laying down notes the
    // whole time it was silent — muting is a level change, not a transport one.
    expect(beforeMute.every((source) => source.stop.mock.calls.length <= 1)).toBe(true)
    expect(FakeSource.all.length).toBeGreaterThan(beforeMute.length)
    audio.stopOriginalCreditsTheme()
  })

  it('is through-composed: it plays out, ends itself, and frees the transport', async () => {
    const audio = await import('./audio')

    expect(audio.startOriginalCreditsTheme()).toBe(true)
    // Past the 87.72 s cue and its ring-out.
    vi.advanceTimersByTime(90_000)

    // Ended on its own, so nothing is left to stop and a later roll starts clean.
    expect(audio.stopOriginalCreditsTheme()).toBe(false)
    expect(audio.startOriginalCreditsTheme()).toBe(true)
    audio.stopOriginalCreditsTheme()
  })

  it('keeps the credits cue inside the four-voice 2A03 budget', async () => {
    const audio = await import('./audio')

    expect(audio.startOriginalCreditsTheme()).toBe(true)
    // Through the intro, both statements, the recap and into the coda.
    vi.advanceTimersByTime(85_000)

    const spans = FakeSource.all
      .filter((source) => source.start.mock.calls.length > 0 && source.stop.mock.calls.length > 0)
      .map((source): [number, number] => [
        source.start.mock.calls[0][0] as number,
        source.stop.mock.calls[0][0] as number,
      ])
    // The cue plans 1091 events; this reaches nearly all of them.
    expect(spans.length).toBeGreaterThan(1_000)

    // Unlike the title score, this cue butt-splices: a note ends exactly where
    // the next one in its channel begins, with no retrigger gap. `time +
    // duration` lands a ULP either side of that boundary, so the sweep needs a
    // tolerance — 1 ms, well under one sample — or every legato handoff reads
    // as an overlap. Channel monophony is what actually holds the ceiling.
    const EPSILON = 1e-3
    let peak = 0
    for (const [onset] of spans) {
      const sounding = spans.filter(
        ([from, until]) => from <= onset + EPSILON && until > onset + EPSILON,
      ).length
      if (sounding > peak) peak = sounding
    }
    expect(peak).toBe(4)

    // Two pulses and a triangle on oscillators, every drum on the LFSR buffer,
    // and nothing else: no sine, no sawtooth, and no filter node to make one.
    const oscillators = FakeSource.all.filter((source) => source.buffer === null)
    expect(new Set(oscillators.map((source) => source.type))).toEqual(
      new Set(['square', 'triangle']),
    )
    expect(FakeAudioContext.instances[0].createBufferSource).toHaveBeenCalled()
    audio.stopOriginalCreditsTheme()
  })
})
