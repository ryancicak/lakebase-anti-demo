import '@testing-library/jest-dom/vitest'

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
    }
  | { kind: 'translate'; x: number; y: number }
  | { kind: 'rotate'; angle: number }

export interface CanvasRecording {
  canvas: HTMLCanvasElement
  operations: CanvasOperation[]
}

const canvasRecordings: CanvasRecording[] = []
const recordingByCanvas = new WeakMap<HTMLCanvasElement, CanvasRecording>()

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
    fillText(text: string, x: number, y: number) {
      recording.operations.push({
        kind: 'fillText',
        text: String(text),
        x,
        y,
        fillStyle: String(context.fillStyle),
        font: context.font,
        textAlign: context.textAlign,
      })
    },
    measureText(text: string) {
      return { width: String(text).length * 8 } as TextMetrics
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
