import { afterEach, describe, expect, it, vi } from 'vitest'
import { api, ApiError, subscribeToSession } from './client'

class EventSourceProbe {
  static instance: EventSourceProbe
  onopen: (() => void) | null = null
  onerror: (() => void) | null = null
  listeners = new Map<string, EventListener>()
  constructor(readonly url: string) { EventSourceProbe.instance = this }
  addEventListener(name: string, listener: EventListener) { this.listeners.set(name, listener) }
  removeEventListener() {}
  close() {}
}

describe('session API client', () => {
  afterEach(() => vi.unstubAllGlobals())

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
      'The demo server could not be reached. Make sure it is running, then try again.',
      0,
    ))
  })

  it('subscribes to every redo event and authoritative cleanup updates', () => {
    vi.stubGlobal('EventSource', EventSourceProbe)
    const onOpen = vi.fn()
    const unsubscribe = subscribeToSession('round-4', vi.fn(), vi.fn(), onOpen)
    expect(EventSourceProbe.instance.url).toBe('/api/sessions/round-4/events')
    expect([...EventSourceProbe.instance.listeners.keys()]).toEqual(expect.arrayContaining([
      'redo_started', 'redo_lane_update', 'redo_finished', 'redo_failed', 'cleanup_update',
    ]))
    EventSourceProbe.instance.onopen?.()
    expect(onOpen).toHaveBeenCalledTimes(1)
    unsubscribe()
    expect(EventSourceProbe.instance.onopen).toBeNull()
  })
})
