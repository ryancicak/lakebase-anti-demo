import { afterEach, describe, expect, it, vi } from 'vitest'
import { api, ApiError, subscribeToSession } from './client'

class EventSourceProbe {
  static instances: EventSourceProbe[] = []
  static instance: EventSourceProbe
  onopen: (() => void) | null = null
  onerror: (() => void) | null = null
  listeners = new Map<string, EventListener>()
  closed = false
  constructor(readonly url: string) {
    EventSourceProbe.instance = this
    EventSourceProbe.instances.push(this)
  }
  addEventListener(name: string, listener: EventListener) { this.listeners.set(name, listener) }
  removeEventListener(name: string) { this.listeners.delete(name) }
  close() { this.closed = true }
  emit(name: string, value: unknown) {
    this.listeners.get(name)?.(new MessageEvent(name, { data: JSON.stringify(value) }))
  }
}

describe('session API client', () => {
  afterEach(() => {
    vi.useRealTimers()
    vi.unstubAllGlobals()
    window.sessionStorage.clear()
    EventSourceProbe.instances = []
  })

  it('posts bodyless redo and cleanup-retry controls', async () => {
    const fetch = vi.fn().mockResolvedValue({ ok: true, json: async () => ({ id: 'round-4' }) })
    vi.stubGlobal('fetch', fetch)
    await api.redoSession('round 4')
    await api.retryCleanup('round 5')
    expect(fetch).toHaveBeenCalledWith('/api/sessions/round%204/redo', expect.objectContaining({ method: 'POST' }))
    expect(fetch.mock.calls[0][1]).not.toHaveProperty('body')
    expect(fetch.mock.calls[0][1].headers).not.toHaveProperty('Content-Type')
    expect(fetch).toHaveBeenCalledWith('/api/sessions/round%205/retry-cleanup', expect.objectContaining({ method: 'POST' }))
    expect(fetch.mock.calls[1][1]).not.toHaveProperty('body')
    expect(fetch.mock.calls[1][1].headers).not.toHaveProperty('Content-Type')
  })

  it('requests status for the selected canonical round', async () => {
    const fetch = vi.fn().mockResolvedValue({ ok: true, json: async () => ({ active: false }) })
    vi.stubGlobal('fetch', fetch)

    await api.boutStatus('make_schema_change_safely')

    expect(fetch).toHaveBeenCalledWith(
      '/api/bout?round_id=make_schema_change_safely',
      expect.objectContaining({ signal: expect.any(AbortSignal) }),
    )
  })

  it('turns a network failure into an actionable API error', async () => {
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new TypeError('Failed to fetch')))

    await expect(api.catalog()).rejects.toEqual(new ApiError(
      'The API could not be reached. Check your connection, then try again.',
      0,
    ))
  })

  it('normalizes FastAPI validation arrays without object coercion', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({
      ok: false,
      status: 422,
      statusText: 'Unprocessable Entity',
      json: async () => ({
        detail: [
          { type: 'extra_forbidden', loc: ['body', 'round_idd'], msg: 'Extra inputs are not permitted' },
          { type: 'enum', loc: ['body', 'corners', 0], msg: 'Input should be a valid priority' },
        ],
      }),
    }))

    await expect(api.createSession({} as never)).rejects.toEqual(new ApiError(
      'round_idd: Extra inputs are not permitted; corners.0: Input should be a valid priority',
      422,
    ))
  })

  it('subscribes to every redo event and authoritative cleanup updates', async () => {
    vi.useFakeTimers()
    vi.stubGlobal('EventSource', EventSourceProbe)
    const onOpen = vi.fn()
    const unsubscribe = subscribeToSession('round-4', vi.fn(), vi.fn(), onOpen, {
      stableAfterMs: 0,
    })
    expect(EventSourceProbe.instance.url).toBe('/api/sessions/round-4/events?after=0')
    expect([...EventSourceProbe.instance.listeners.keys()]).toEqual(expect.arrayContaining([
      'redo_started', 'redo_lane_update', 'redo_finished', 'redo_failed', 'cleanup_update',
      'stream_rotate',
    ]))
    EventSourceProbe.instance.onopen?.()
    await vi.advanceTimersByTimeAsync(0)
    expect(onOpen).toHaveBeenCalledTimes(1)
    unsubscribe()
    expect(EventSourceProbe.instance.onopen).toBeNull()
    expect(EventSourceProbe.instance.closed).toBe(true)
  })

  it('reconnects from the high-water sequence without duplicates or concurrent streams', async () => {
    vi.useFakeTimers()
    vi.stubGlobal('EventSource', EventSourceProbe)
    const events = vi.fn()
    const errors = vi.fn()
    const unsubscribe = subscribeToSession('bout-1', events, errors, vi.fn(), {
      baseDelayMs: 100,
      maxDelayMs: 1000,
      random: () => 0,
    })
    const first = EventSourceProbe.instance
    first.emit('lane_update', { sequence: 7, event: 'lane_update', payload: {} })
    first.onerror?.()

    expect(first.closed).toBe(true)
    expect(errors).toHaveBeenCalledWith({ kind: 'transport', attempts: 1, permanent: false })
    await vi.advanceTimersByTimeAsync(99)
    expect(EventSourceProbe.instances).toHaveLength(1)
    await vi.advanceTimersByTimeAsync(1)
    const second = EventSourceProbe.instance
    expect(second.url).toBe('/api/sessions/bout-1/events?after=7')
    expect(EventSourceProbe.instances.filter((source) => !source.closed)).toEqual([second])

    second.emit('lane_update', { sequence: 7, event: 'lane_update', payload: {} })
    second.emit('lane_update', { sequence: 8, event: 'lane_update', payload: {} })
    expect(events.mock.calls.map(([event]) => event.sequence)).toEqual([7, 8])
    unsubscribe()
  })

  it('rotates a completed stream immediately without reporting an interruption', async () => {
    vi.useFakeTimers()
    vi.stubGlobal('EventSource', EventSourceProbe)
    const errors = vi.fn()
    subscribeToSession('bout-rotate', vi.fn(), errors)
    const first = EventSourceProbe.instance
    first.emit('stream_rotate', { sequence: 12 })
    first.onerror?.()
    await vi.advanceTimersByTimeAsync(0)

    expect(errors).not.toHaveBeenCalled()
    expect(first.closed).toBe(true)
    expect(EventSourceProbe.instance.url).toBe('/api/sessions/bout-rotate/events?after=12')
    expect(EventSourceProbe.instances.filter((source) => !source.closed)).toHaveLength(1)
  })

  it('uses exponential jittered backoff and exposes repeated permanent failure', async () => {
    vi.useFakeTimers()
    vi.stubGlobal('EventSource', EventSourceProbe)
    const errors = vi.fn()
    subscribeToSession('bout-down', vi.fn(), errors, vi.fn(), {
      baseDelayMs: 100,
      maxDelayMs: 1000,
      permanentAfter: 3,
      stableAfterMs: 10_000,
      random: () => 1,
    })

    EventSourceProbe.instance.onerror?.()
    await vi.advanceTimersByTimeAsync(125)
    EventSourceProbe.instance.onerror?.()
    await vi.advanceTimersByTimeAsync(250)
    EventSourceProbe.instance.onerror?.()

    expect(errors.mock.calls.map(([failure]) => failure)).toEqual([
      { kind: 'transport', attempts: 1, permanent: false },
      { kind: 'transport', attempts: 2, permanent: false },
      { kind: 'transport', attempts: 3, permanent: true },
    ])
  })

  it('does not declare a reconnect healthy until it stays open', async () => {
    vi.useFakeTimers()
    vi.stubGlobal('EventSource', EventSourceProbe)
    const onOpen = vi.fn()
    subscribeToSession('bout-flapping', vi.fn(), vi.fn(), onOpen, {
      baseDelayMs: 10,
      stableAfterMs: 1000,
      random: () => 0,
    })

    EventSourceProbe.instance.onopen?.()
    await vi.advanceTimersByTimeAsync(999)
    expect(onOpen).not.toHaveBeenCalled()
    await vi.advanceTimersByTimeAsync(1)
    expect(onOpen).toHaveBeenCalledTimes(1)

    EventSourceProbe.instance.onerror?.()
    await vi.advanceTimersByTimeAsync(10)
    EventSourceProbe.instance.onopen?.()
    await vi.advanceTimersByTimeAsync(500)
    EventSourceProbe.instance.onerror?.()
    expect(onOpen).toHaveBeenCalledTimes(1)
  })

  it('closes on the terminal cooldown event and resumes across a page remount', () => {
    vi.stubGlobal('EventSource', EventSourceProbe)
    const firstEvents = vi.fn()
    const firstUnsubscribe = subscribeToSession('bout-terminal', firstEvents, vi.fn())
    EventSourceProbe.instance.emit('lane_update', {
      sequence: 20, event: 'lane_update', payload: {},
    })
    firstUnsubscribe()

    const terminalEvents = vi.fn()
    subscribeToSession('bout-terminal', terminalEvents, vi.fn())
    const terminalSource = EventSourceProbe.instance
    expect(terminalSource.url).toBe('/api/sessions/bout-terminal/events?after=20')
    terminalSource.emit('cooldown_ready', {
      sequence: 21, event: 'cooldown_ready', payload: {},
    })
    terminalSource.onerror?.()

    expect(terminalEvents).toHaveBeenCalledTimes(1)
    expect(terminalSource.closed).toBe(true)
    expect(EventSourceProbe.instances.filter((source) => !source.closed)).toHaveLength(0)
  })

  it('does not swallow malformed application events', () => {
    vi.stubGlobal('EventSource', EventSourceProbe)
    const errors = vi.fn()
    subscribeToSession('bout-protocol', vi.fn(), errors, vi.fn(), {
      permanentAfter: 1,
    })

    EventSourceProbe.instance.emit('lane_update', { event: 'lane_update', payload: {} })

    expect(errors).toHaveBeenCalledWith({ kind: 'protocol', attempts: 1, permanent: true })
    expect(EventSourceProbe.instance.closed).toBe(true)
  })
})
