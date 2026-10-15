import '@testing-library/jest-dom/vitest'
import { beforeEach } from 'vitest'
import { halloweenClock } from '../halloween'

const localStorageValues = new Map<string, string>()

export type CanvasOperation =
  | {
      kind: 'clearRect'
      x: number
      y: number
      width: number
      height: number
    }
  | {
      kind: 'fillRect'
      x: number
      y: number
      width: number
      height: number
      fillStyle: string
      strokeStyle: string
      lineWidth: number
    }
  | {
      kind: 'strokeRect'
      x: number
      y: number
      width: number
      height: number
      fillStyle: string
      strokeStyle: string
      lineWidth: number
    }
  | {
      kind: 'fillText'
      text: string
      x: number
      y: number
      fillStyle: string
      font: string
      textAlign: CanvasTextAlign
      textBaseline: CanvasTextBaseline
      /** The width the browser would condense the text into, when the call passed one. */
      maxWidth?: number
    }
  | { kind: 'translate'; x: number; y: number }
  | { kind: 'rotate'; angle: number }

export interface CanvasRecording {
  canvas: HTMLCanvasElement
  operations: CanvasOperation[]
}

const canvasRecordings: CanvasRecording[] = []
const recordingByCanvas = new WeakMap<HTMLCanvasElement, CanvasRecording>()

/**
 * The pixel size a canvas `font` string sets, e.g. 34 for `400 34px "Press Start 2P", monospace`.
 * A font this mock cannot read is an error, not a guess, so a card test can't measure nonsense.
 */
export function canvasFontSize(font: string): number {
  const match = /(\d+(?:\.\d+)?)px/.exec(font)
  if (!match) throw new Error(`Canvas font without a pixel size: ${font}`)
  return Number(match[1])
}

/**
 * How wide a real browser draws `text` in the cards' font. Press Start 2P gives every glyph
 * exactly one em of advance (its hmtx has 1000 units per 1000-unit em for each glyph), so
 * this is the browser's own measure, not an estimate. The old stub's flat 8 px a character
 * at every size is how a 90 px hero overflowing its column passed every card test.
 */
export function measuredTextWidth(text: string, font: string): number {
  return Array.from(text).length * canvasFontSize(font)
}

export function resetCanvasRecordings(): void {
  canvasRecordings.length = 0
}

export function getCanvasRecordings(): readonly CanvasRecording[] {
  return canvasRecordings
}

function recordingContext(recording: CanvasRecording): CanvasRenderingContext2D {
  const state = {
    fillStyle: '#000000',
    strokeStyle: '#000000',
    lineWidth: 1,
    font: '10px sans-serif',
    textAlign: 'start' as CanvasTextAlign,
    textBaseline: 'alphabetic' as CanvasTextBaseline,
    globalAlpha: 1,
    imageSmoothingEnabled: true,
  }
  const stack: Array<typeof state> = []
  const context = {
    ...state,
    clearRect(x: number, y: number, width: number, height: number) {
      recording.operations.push({ kind: 'clearRect', x, y, width, height })
    },
    fillRect(x: number, y: number, width: number, height: number) {
      recording.operations.push({
        kind: 'fillRect',
        x,
        y,
        width,
        height,
        fillStyle: String(context.fillStyle),
        strokeStyle: String(context.strokeStyle),
        lineWidth: context.lineWidth,
      })
    },
    strokeRect(x: number, y: number, width: number, height: number) {
      recording.operations.push({
        kind: 'strokeRect',
        x,
        y,
        width,
        height,
        fillStyle: String(context.fillStyle),
        strokeStyle: String(context.strokeStyle),
        lineWidth: context.lineWidth,
      })
    },
    fillText(text: string, x: number, y: number, maxWidth?: number) {
      recording.operations.push({
        kind: 'fillText',
        text: String(text),
        x,
        y,
        fillStyle: String(context.fillStyle),
        font: context.font,
        textAlign: context.textAlign,
        textBaseline: context.textBaseline,
        ...(maxWidth === undefined ? {} : { maxWidth }),
      })
    },
    measureText(text: string) {
      return { width: measuredTextWidth(String(text), context.font) } as TextMetrics
    },
    save() {
      stack.push({
        fillStyle: context.fillStyle,
        strokeStyle: context.strokeStyle,
        lineWidth: context.lineWidth,
        font: context.font,
        textAlign: context.textAlign,
        textBaseline: context.textBaseline,
        globalAlpha: context.globalAlpha,
        imageSmoothingEnabled: context.imageSmoothingEnabled,
      })
    },
    restore() {
      const restored = stack.pop()
      if (restored) Object.assign(context, restored)
    },
    translate(x: number, y: number) {
      recording.operations.push({ kind: 'translate', x, y })
    },
    rotate(angle: number) {
      recording.operations.push({ kind: 'rotate', angle })
    },
  }
  const knownMembers = new Set(Reflect.ownKeys(context))
  return new Proxy(context, {
    get(target, property, receiver) {
      if (knownMembers.has(property)) return Reflect.get(target, property, receiver)
      throw new Error(`Unexpected CanvasRenderingContext2D member: ${String(property)}`)
    },
    set(target, property, value, receiver) {
      if (!knownMembers.has(property)) {
        throw new Error(`Unexpected CanvasRenderingContext2D member: ${String(property)}`)
      }
      return Reflect.set(target, property, value, receiver)
    },
  }) as unknown as CanvasRenderingContext2D
}

Object.defineProperty(HTMLCanvasElement.prototype, 'getContext', {
  configurable: true,
  value(this: HTMLCanvasElement, contextId: string) {
    if (contextId !== '2d') {
      throw new Error(`Unexpected canvas context requested in test: ${contextId}`)
    }
    let recording = recordingByCanvas.get(this)
    if (!recording) {
      recording = { canvas: this, operations: [] }
      recordingByCanvas.set(this, recording)
      canvasRecordings.push(recording)
    } else if (!canvasRecordings.includes(recording)) {
      canvasRecordings.push(recording)
    }
    return recordingContext(recording)
  },
})

Object.defineProperty(HTMLCanvasElement.prototype, 'toBlob', {
  configurable: true,
  value(
    this: HTMLCanvasElement,
    callback: BlobCallback,
    type = 'image/png',
  ) {
    const recording = recordingByCanvas.get(this)
    if (!recording || recording.operations.length === 0) {
      callback(null)
      return
    }
    callback(new Blob([JSON.stringify(recording.operations)], { type }))
  },
})

Object.defineProperty(window, 'localStorage', {
  configurable: true,
  value: {
    getItem: (key: string) => localStorageValues.get(key) ?? null,
    setItem: (key: string, value: string) => { localStorageValues.set(key, String(value)) },
    removeItem: (key: string) => { localStorageValues.delete(key) },
    clear: () => { localStorageValues.clear() },
    key: (index: number) => [...localStorageValues.keys()][index] ?? null,
    get length() { return localStorageValues.size },
  },
})

Object.defineProperty(window, 'matchMedia', {
  writable: true,
  value: (query: string) => ({
    matches: false,
    media: query,
    onchange: null,
    addListener: () => undefined,
    removeListener: () => undefined,
    addEventListener: () => undefined,
    removeEventListener: () => undefined,
    dispatchEvent: () => false,
  }),
})

// Halloween (v1.2.1): every test starts outside the season, so none depends on the month the
// suite runs in. A Halloween test moves `halloweenClock` into October itself; this puts it
// back before the next test.
const OUTSIDE_HALLOWEEN = (): Date => new Date(2026, 5, 15, 12, 0, 0)
halloweenClock.now = OUTSIDE_HALLOWEEN
beforeEach(() => { halloweenClock.now = OUTSIDE_HALLOWEEN })
