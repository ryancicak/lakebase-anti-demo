export function compactDuration(milliseconds: number): string {
  // Claims are deliberately conservative at whole-second precision. Flooring
  // prevents a 143.6s raw advantage from being promoted to a visually
  // inconsistent "2m 24s" beside clocks that read 0:01 and 2:24.
  const totalSeconds = Math.max(0, Math.floor(milliseconds / 1000))
  if (totalSeconds < 60) {
    return milliseconds < 10_000
      ? `${(milliseconds / 1000).toFixed(2)}s`
      : `${totalSeconds}s`
  }
  const minutes = Math.floor(totalSeconds / 60)
  const seconds = totalSeconds % 60
  return `${minutes}m ${seconds.toString().padStart(2, '0')}s`
}

export function preciseDuration(milliseconds: number): string {
  return `${(Math.max(0, milliseconds) / 1000).toFixed(2)}s`
}
