/**
 * Halloween (v1.2.1): the season, the share card's round names and pixel art, and the
 * boxers' costumes.
 *
 * In season the Share dialog offers, checked, a Halloween card: the same receipt, the same
 * exact clocks, the same verdict and the same house rules, in a Halloween skin. The live
 * boxers wear costumes on their heads; no word, number or colour the room reads changes.
 *
 * The season is October 1 through November 1 by the viewer's own date. The rest of the year
 * nothing changes. The art follows Ryan's "Twelve Nights of Fright" design, without its
 * series branding.
 */

/**
 * What a boxer wears in season. Lakebase is the pumpkin. The opponent, Aurora or RDS, is
 * Frankenstein's monster in every round and on every card, because it is the same opponent
 * all night; a new costume each round would read as a new opponent.
 */
export type FighterCostume = 'pumpkin' | 'frankenstein'

/** Each round's Halloween name, printed beside its number: a pun on what the round proves. */
export const HALLOWEEN_ROUND_NAMES: Readonly<Record<number, string>> = {
  1: 'Wake the dead',
  2: 'Stitch the monster',
  3: 'Raise it from the grave',
  4: 'Summon the score',
  5: 'Ten thousand ghosts',
  6: 'Feed the lake',
}

/**
 * The clock the season is read from. The test setup pins it outside the season, so no test
 * depends on the month it runs in; a Halloween test sets it, and restores it.
 */
export const halloweenClock = { now: (): Date => new Date() }

/** Whether ``now`` falls in the Halloween season: October 1 through November 1, local time. */
export function isHalloweenSeason(now: Date = halloweenClock.now()): boolean {
  const month = now.getMonth()
  return month === 9 || (month === 10 && now.getDate() === 1)
}

/**
 * What a live boxer wears, or nothing: in season, the red corner's pumpkin and the blue
 * corner's Frankenstein. ``?costumes=off`` turns them off, for a presenter who wants the room
 * without the wink.
 */
export function fighterCostume(
  corner: 'red' | 'blue',
  search: string = typeof window === 'undefined' ? '' : window.location.search,
): FighterCostume | undefined {
  if (!isHalloweenSeason() || new URLSearchParams(search).get('costumes') === 'off') return undefined
  return corner === 'red' ? 'pumpkin' : 'frankenstein'
}

/** The design's palette. */
export const HALLOWEEN = {
  ink: '#0d0818',
  night: '#1c1133',
  stripe: 'rgba(0, 0, 0, 0.28)',
  frame: '#ff8a1f',
  border: '#3b1f6b',
  ring: '#5b35b5',
  pumpkin: '#ff8a1f',
  candy: '#ffc531',
  blood: '#e8432e',
  bruise: '#7a1b26',
  bone: '#f4ecd8',
  lilac: '#b9a7e6',
  track: '#241741',
  trackEdge: '#4a3580',
  slime: '#2ecc71',
  crypt: '#0f2d1f',
  aurora: '#3b7bea',
  rds: '#2aa3b8',
} as const

const SPRITE_PALETTE: Readonly<Record<string, string>> = {
  k: '#0d0818', o: '#ff8a1f', g: '#4caf50', z: '#9ad46a', n: '#9aa0a6',
}

/** 16 x 12 heads: Lakebase's pumpkin, and the opponent's Frankenstein, scar, bolts and all. */
const HEADS: Readonly<Record<FighterCostume, readonly string[]>> = {
  pumpkin: ['.......gg.......', '.......gg.......', '....oooooooo....', '..oooooooooooo..', '.oooooooooooooo.', '.oookooooookooo.', '.ookkkooookkkoo.', '.oooooooooooooo.', '.ookoookkoookoo.', '.ookkkkkkkkkkoo.', '..oooooooooooo..', '....oooooooo....'],
  frankenstein: ['..kkkkkkkkkkkk..', '..kkkkkkkkkkkk..', '..kkkkkkkkkkkk..', '..zzzzzzzkzkzz..', '..zzkkzzzzkkzz..', '..zzkkzzzzkkzz..', '..zzzzzzzzzzzz..', '..zzzkkkkkkzzz..', '..zzzzzzzzzzzz..', '.nzzzzzzzzzzzzn.', '....zzzzzzzz....', '......zzzz......'],
}
/** 16 x 8 body: ``b`` is the fighter's colour, ``H`` its hands (none showing on the pumpkin). */
const BODY = ['....bbbbbbbb....', '.bbbbbbbbbbbbbb.', 'HbbbbbbbbbbbbbbH', 'HbbbbbbbbbbbbbbH', '...bbbbbbbbbb...', '...bbbbbbbbbb...', '...bbbbbbbbbb...', '...bbbbbbbbbb...']
const HANDS: Readonly<Record<FighterCostume, string>> = { pumpkin: '.', frankenstein: 'z' }
const BAT = ['k.........k', 'kk...k...kk', 'kkkkkkkkkkk', '.kkk.k.kkk.', '..k.....k..']
const TOMB = ['..ttt..', '.ttttt.', 'ttttttt', 'ttt.ttt', 'tt...tt', 'ttt.ttt', 'ttt.ttt', 'ttttttt', 'ttttttt']

/** A pixel grid: its width in cells and one colour (or ``null``, transparent) per cell. */
export interface PixelGrid {
  columns: number
  cells: (string | null)[]
}

function grid(rows: readonly string[], palette: Readonly<Record<string, string>>): PixelGrid {
  return {
    columns: rows[0].length,
    cells: rows.join('').split('').map((cell) => palette[cell] ?? null),
  }
}

/** Lakebase's pumpkin fighter, or the opponent's Frankenstein, in ``body`` colour. */
export function fighterSprite(costume: FighterCostume, body: string): PixelGrid {
  return grid(
    [...HEADS[costume], ...BODY.map((row) => row.replace(/H/g, HANDS[costume]))],
    { ...SPRITE_PALETTE, b: body },
  )
}

export const BAT_SPRITE: PixelGrid = grid(BAT, { k: '#5b3fa3' })
export const TOMB_SPRITE: PixelGrid = grid(TOMB, { t: '#2e1d52' })

/** The cratered moon: a stepped disc on an ``n`` x ``n`` grid. */
export function moonSprite(n = 22): PixelGrid {
  const centre = (n - 1) / 2
  const craters: [number, number, number][] = [[7, 8, 1.6], [14, 13, 2.2], [9, 15, 1.1], [15, 6, 0.9]]
  const cells: (string | null)[] = []
  for (let y = 0; y < n; y++) {
    for (let x = 0; x < n; x++) {
      if (Math.hypot(x - centre, y - centre) > 10.6) cells.push(null)
      else if (craters.some(([cx, cy, r]) => Math.hypot(x - cx, y - cy) <= r)) cells.push('#d9c48c')
      else cells.push('#f1e0ad')
    }
  }
  return { columns: n, cells }
}

/** The pumpkin glow behind the left of the card: three soft rings. */
export function glowSprite(n = 26): PixelGrid {
  const centre = (n - 1) / 2
  const cells: (string | null)[] = []
  for (let y = 0; y < n; y++) {
    for (let x = 0; x < n; x++) {
      const d = Math.hypot(x - centre, y - centre)
      cells.push(
        d <= 6 ? 'rgba(255,138,31,0.32)'
          : d <= 9.5 ? 'rgba(255,138,31,0.18)'
          : d <= 13 ? 'rgba(255,138,31,0.09)'
          : null,
      )
    }
  }
  return { columns: n, cells }
}

/** Paints ``sprite`` with its top-left at (``x``, ``y``), each cell ``cell`` px square. */
export function drawPixelGrid(
  context: CanvasRenderingContext2D,
  sprite: PixelGrid,
  x: number,
  y: number,
  cell: number,
): void {
  sprite.cells.forEach((colour, index) => {
    if (!colour) return
    context.fillStyle = colour
    context.fillRect(
      x + (index % sprite.columns) * cell,
      y + Math.floor(index / sprite.columns) * cell,
      cell,
      cell,
    )
  })
}
