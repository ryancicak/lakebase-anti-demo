// The credits roll. Opened from credits-entry.tsx, which owns the portal, the
// exit, and the focus round trip; this file owns only what is on screen.
//
// Visual direction: the layout language of a late-80s arcade boxing ending —
// dark bobbing crowd, cream ropes, chunky centered pixel type, fighters in
// opposing corners, referee center-stage, a slow vertical crawl. The arcade read
// comes from structure and motion; the palette is the app's own navy :root
// tokens, so no recognizable color signature is borrowed either. Every mark on
// screen is drawn procedurally here or is an original asset already in brand/.
// No art, sprites, wordmarks or traced shapes from any commercial arcade boxing
// title are used, referenced or redrawn here.
//
// NO FICTIONAL PEOPLE. There is no film-style STARRING / FEATURING billing any
// more. It used to print the bout's selected personas — Lockjaw Lucy, 3 A.M.
// Sam — and personas are the demo's cast, not its crew. Credits are for the
// people who built the thing, so the GitHub contributors are the centerpiece and
// they are billed second, directly under the title card. Do not reintroduce
// characters here under any other heading.

import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type KeyboardEvent as ReactKeyboardEvent,
  type ReactNode,
} from 'react'
import { brandAssets } from './assets'
import contributorsData from './credits-contributors.json'
import {
  playConfirm,
  playCursor,
  playOriginalBell,
  startOriginalCreditsTheme,
  startOriginalTitleTheme,
  stopOriginalCreditsTheme,
  stopOriginalTitleTheme,
} from './audio'
import { CREDITS_THEME_BELL_SECONDS } from './credits-score'
import { useReducedMotion } from './hooks/useReducedMotion'
import type { CreditsTally } from './credits-tally'
import type { CompetitorDefinition } from './api/types'
import './credits.css'
/* The authorship card and the held outro are styled here, not in credits.css.
   Both sheets are hand-authored and they split the roll between them:
   credits.css dresses the scene and everything that scrolls through it, this
   one dresses the card, the held mark and the entry control. The
   import is explicit because the roll now depends on it: credits-entry.tsx also
   imports this sheet, but <Credits> is rendered directly by tests and by the
   audit harness, and without this line the author credit falls back to unstyled
   defaults in exactly those places. Bundlers dedupe the second import. */
import './credits-entry.css'

/** Reference crawl speed. Only used when there is no cue to sync to. */
const DESIGN_PX_PER_SECOND = 40

export const CONTRIBUTE_REPO = 'github.com/ryancicak/lakebase-anti-demo'
/** Derived so the displayed text and the href cannot drift apart. The roll
 *  prints the bare host+path -- a scheme reads as a paste, not as a title
 *  card -- and the anchor supplies the scheme. */
export const CONTRIBUTE_URL = `https://${CONTRIBUTE_REPO}`

/**
 * The repo string cut after every `/` and `-`, so the roll can put a `<wbr>`
 * between the pieces.
 *
 * WHY. At 390px the invitation card is 179px wide and the repo is a single
 * 39-character word, so `overflow-wrap: break-word` breaks it wherever the line
 * happens to run out. That landed mid-handle -- `github.com/ryanci` /
 * `cak/lakebase-` -- and this link is the one thing on the roll a viewer is
 * meant to act on, so it has to break where a reader would expect it to.
 *
 * WHY NOT CSS. Measured in Chromium at 390px: `word-break: break-word` and
 * `word-break: break-all` both produce exactly the same break as the baseline.
 * Neither adds a break opportunity; they only license the arbitrary one that
 * was already being taken. There is no CSS property that says "prefer the
 * slashes", so the opportunities have to come from the markup.
 *
 * `<wbr>` is empty and childless, so `textContent` and the accessible name are
 * still exactly CONTRIBUTE_REPO -- the link is not truncated, not ellipsized
 * and not shrunk. `overflow-wrap` stays on as the fallback for a viewport
 * narrower than the longest single segment.
 *
 * Mirrored in `desktop-build/build.py`, which renders the same card standalone.
 */
const CONTRIBUTE_SEGMENTS = CONTRIBUTE_REPO.split(/(?<=[/-])/)

/* THE ACT LIST IS GONE. A "The card" block used to bill the six rounds between
 * the crew and the corners, off a ROUNDS constant. It was cut because it
 * credited nobody: it restated the running order of the thing the audience had
 * just watched, and it was the one block on the roll whose job was to fill
 * space. Removing it also took the roll's only copy of ROUNDS.md's contested
 * Round 5 title with it, so there is one fewer place for that string to drift.
 *
 * Do not reintroduce a round list here. If the roll needs another beat, it needs
 * one that credits someone. */

/**
 * Build-time snapshot, never a runtime fetch.
 *
 * `contributors/fetch-contributors.py` resolves the list once and commits it;
 * copy that file to `frontend/src/credits-contributors.json`. The roll must not
 * call api.github.com: the demo runs on conference wifi and inside customer
 * VPNs with no egress to github.com, anonymous GitHub is 60 requests/hour per
 * IP (one NAT address for a whole room), and the credits are the single screen
 * where a spinner is least acceptable -- they play after the verdict and there
 * is nothing to retry.
 *
 * The committed list holds ONE name today: the repo does not exist yet and the
 * working tree is not a git repository, so there is no history to read, and the
 * project's sole author is seeded instead. That is why `crewScale` exists -- one
 * name has to look like a title card, not like a list that lost its other rows.
 * An empty list still drops the block entirely rather than render an empty
 * header, and there are deliberately no placeholder names: five invented logins
 * in the crew block is the same fictional-people problem as the persona billing.
 */
interface Contributor {
  login: string
  name: string | null
  html_url: string
}

/**
 * Type scale bucket for the crew list. Mirrors `crew_scale()` in
 * `desktop-build/build.py`; keep the thresholds identical.
 */
function crewScale(count: number): 'solo' | 'few' | 'many' | 'crowd' {
  if (count <= 1) return 'solo'
  if (count <= 6) return 'few'
  if (count <= 16) return 'many'
  return 'crowd'
}

export function Credits({
  competitors,
  tally,
  sound,
  boutInFlight = false,
  onBack,
}: {
  competitors: CompetitorDefinition[]
  tally: CreditsTally
  sound: boolean
  /**
   * True while a bout is actually running underneath this overlay.
   *
   * Suppresses the finale bell and shows a standing note. It does not disable
   * anything: the roll is an overlay rendered in addition to the current
   * screen, never instead of it, so it cannot disturb a live bout.
   */
  boutInFlight?: boolean
  /** Closes the roll. */
  onBack: () => void
}) {
  const reducedMotion = useReducedMotion()
  const frameRef = useRef<HTMLDivElement | null>(null)
  const rollRef = useRef<HTMLDivElement | null>(null)
  const [rolled, setRolled] = useState(reducedMotion)

  /* THE MUSIC POLICY: the roll asks for its own cue and takes no for an answer.
   *
   * An earlier draft of this file started the TITLE theme on mount and stopped
   * it on unmount. That was wrong for a reason that only shows up once the roll
   * is reachable while the arena is live: the operator can open it while a round
   * theme is playing under a live bout, and there is no honest way to give that
   * back. The cues are start/stop with no seek, so "restore on exit" means
   * restarting the round theme from bar one in the middle of a run, which is
   * more disruptive than never having touched it.
   *
   * The roll now has a cue of its own -- "The Long Walk Back", 40 bars,
   * through-composed -- and audio.ts owns the whole policy:
   * startOriginalCreditsTheme() returns false while a round cue holds the
   * transport and plays nothing at all, so a bout is never interrupted and this
   * component needs no test for it. The title score is the one thing the cue
   * outranks, because it is a stateless attract loop that is restarted from bar
   * one every time the app lands on the title screen anyway. That is the only
   * case with anything to give back, which is why the boolean from stopping it
   * is what decides whether it gets handed back on the way out.
   *
   * One-shot blips still fire, because they cannot collide with a running cue
   * and cannot be left running. The bell is the exception: it is the app's own
   * signal that a round ended, so it is suppressed over a live bout. */
  const soundAtOpen = useRef(sound)
  /* Whether the cue actually took the transport, read by the roll's clock
     below. A ref rather than state because the clock has to see the answer in
     the same commit: effects run in declaration order, so this one settles it
     before the one that measures and starts the crawl. */
  const cueRunning = useRef(false)
  useEffect(() => {
    // Mount only. `sound` is a mute, not a transport control, so a presenter
    // muting mid-roll must not tear the cue down; App.tsx ramps the master
    // instead and the cue keeps its place.
    if (!soundAtOpen.current) return
    const titleWasPlaying = stopOriginalTitleTheme()
    if (!startOriginalCreditsTheme()) {
      if (titleWasPlaying) startOriginalTitleTheme()
      return
    }
    cueRunning.current = true
    return () => {
      cueRunning.current = false
      stopOriginalCreditsTheme()
      if (titleWasPlaying) startOriginalTitleTheme()
    }
  }, [])
  const finish = useCallback(() => {
    setRolled((already) => {
      if (!already && sound && !boutInFlight) playOriginalBell()
      return true
    })
  }, [boutInFlight, sound])

  const skip = useCallback(() => {
    if (rolled) return
    if (sound) playCursor()
    finish()
  }, [finish, rolled, sound])

  const exit = useCallback(() => {
    if (sound) playConfirm()
    onBack()
  }, [onBack, sound])

  /* ---------------------------------------------------------------- *
   * THE ROLL'S CLOCK, AND THE MUSIC IS THE MASTER.
   *
   * The crawl used to run at a fixed 40 px/s and let its duration float with
   * content height. That is backwards for a scored crawl, and it is exactly
   * what breaks when a block is cut: a shorter roll at a fixed speed finishes
   * early and sits on dead air while the cue plays on. So when the cue actually
   * took the transport the roll is pinned to CREDITS_THEME_BELL_SECONDS -- bar
   * 40's downbeat, the final D major, which credits-score.ts exports for this
   * purpose -- and the scroll speed is derived from whatever height the roll
   * turned out to have. Cutting "The card" now slows the crawl by that block's
   * height instead of ending the picture before the music. 40 px/s survives as
   * the reference speed for the one case with nothing to sync to: `sound` was
   * off when the roll opened, so there is no cue.
   *
   * Driven from here rather than by a CSS animation, which is what credits.css
   * documents and what the standalone build does. The travel is not known until
   * the roll has been measured, and translateY(100%) would be 100% of the
   * ROLL's height rather than the frame's -- doubling the distance while the
   * duration still only covered (roll + frame), which is the double-speed bug
   * the keyframe comment warns about. Starting at +frameHeight px and ending at
   * -rollHeight px is the same travel the duration is computed from.
   * ---------------------------------------------------------------- */
  const finishRef = useRef(finish)
  useEffect(() => { finishRef.current = finish }, [finish])
  useEffect(() => {
    const frame = frameRef.current
    const roll = rollRef.current
    if (reducedMotion || rolled || !frame || !roll) return
    const lead = frame.clientHeight
    const travel = roll.scrollHeight + lead
    const seconds = cueRunning.current
      ? CREDITS_THEME_BELL_SECONDS
      : travel / DESIGN_PX_PER_SECOND
    // jsdom, and any layout that has not happened yet, measure zero.
    if (travel <= 0 || seconds <= 0) return

    let request = 0
    // Null rather than 0: a first frame timestamped 0 is legal, and treating it
    // as "not started yet" would re-zero the clock on the following frame.
    let startedAt: number | null = null
    const step = (now: number) => {
      if (startedAt === null) startedAt = now
      const progress = Math.min(1, (now - startedAt) / (seconds * 1000))
      roll.style.transform = `translateY(${lead - travel * progress}px)`
      if (progress < 1) {
        request = requestAnimationFrame(step)
        return
      }
      finishRef.current()
    }
    request = requestAnimationFrame(step)
    return () => cancelAnimationFrame(request)
  }, [reducedMotion, rolled])

  /* Keys are bound to this subtree, NOT to window.
   *
   * The roll is an overlay over a still-mounted, still-live arena. A window
   * listener for bare "b" would swallow that key from anything underneath, and
   * Enter / Space are exactly the keys the arena's own controls use. Binding to
   * the container means these only fire while focus is inside the roll, which is
   * where CreditsButton puts it on open. Escape is handled one level up, by
   * CreditsButton, following the ApiIndicator convention already in App.tsx --
   * so there is one Escape handler for this feature, not two. */
  const onKeyDown = useCallback((event: ReactKeyboardEvent<HTMLElement>) => {
    if (event.key === 'b' || event.key === 'B') {
      event.preventDefault()
      exit()
      return
    }
    if (event.key === 'Enter' || event.key === ' ') {
      event.preventDefault()
      skip()
    }
  }, [exit, skip])

  /**
   * Alphabetical by login, case-insensitive. Deliberately not by commit count,
   * and the fetch script does not even write the counts -- a leaderboard in a
   * credits roll creates a hierarchy people measure themselves against, which
   * is the opposite of a thank-you. Re-sorted here so a hand-edited JSON file
   * cannot reintroduce an ordering that means something.
   */
  const contributors = useMemo<Contributor[]>(
    () => [...((contributorsData as { contributors?: Contributor[] }).contributors ?? [])]
      .filter((person) => person?.login)
      .sort((a, b) => a.login.toLowerCase().localeCompare(b.login.toLowerCase())),
    [],
  )
  const opponents = useMemo(
    () => competitors.filter((competitor) => tally.competitors.includes(competitor.short_name)),
    [competitors, tally.competitors],
  )

  return (
    <main
      className="retro-screen credits-arena"
      aria-labelledby="credits-heading"
      onKeyDown={onKeyDown}
    >
      <h1 className="sr-only" id="credits-heading">Credits</h1>

      {/* Procedural crowd: two ranks of tiny pixel heads on a saturated arena wall. */}
      <div className="credits-crowd" data-still={reducedMotion} aria-hidden="true" />
      <div className="credits-apron" aria-hidden="true" />

      {/* Ropes and turnbuckles frame the roll so it reads as happening on the ring canvas. */}
      <div className="credits-ropes credits-ropes-top" aria-hidden="true">
        <span /><span /><span />
      </div>
      <div className="credits-ropes credits-ropes-bottom" aria-hidden="true">
        <span /><span /><span />
      </div>
      <div className="credits-post credits-post-left" aria-hidden="true"><i /><i /><i /></div>
      <div className="credits-post credits-post-right" aria-hidden="true"><i /><i /><i /></div>

      <PixelCornerFighter className="credits-fighter-red" color="#e8482e" label="LB" />
      <PixelCornerFighter className="credits-fighter-blue" color="#4a83e8" label="OPP" />

      <div
        className="credits-frame"
        ref={frameRef}
        data-rolled={rolled}
        data-static={reducedMotion}
        onClick={skip}
      >
        <div className="credits-roll" ref={rollRef}>
          <CreditsTitle tally={tally} />

          {/* THE HOOK LEADS. It used to sit ninth of ten, between the fairness
              card and the author credit, which is the last place an audience is
              still reading. It is now the first thing after the title.
              WHY IT, AND NOT THE CREW. The obvious way to make this roll less
              of a solo show is to promote the contributors, and that is the one
              move that backfires: credits-contributors.json holds exactly one
              name today -- the repo does not exist yet, so there is no history
              to fetch -- and `crewScale` bills a single name as a hero card. So
              leading with the crew would open the roll on one man's name in a
              bordered plate, which is more of a solo show, not less. The
              invitation is the only beat on the roll that is about somebody
              other than whoever has already committed, so it is the one that
              earns the top slot. When the fetch script does replace this list,
              the crew block directly beneath it grows into the space and the
              order still reads correctly.
              This is a MOVE, not a copy: there is exactly one invitation and
              exactly one repo link on the roll. The <wbr> segmentation and the
              pinned grid tracks travel with the markup unchanged. */}
          <div className="credits-invite">
            <p className="pixel-kicker">Now your turn</p>
            <div>
              {/* The one thing on the roll a viewer is meant to act on, so it is
                  a real anchor rather than a line of text to retype. New tab
                  because the roll is an overlay over a possibly-live arena and
                  navigating this one away would take the bout with it.
                  stopPropagation because the frame's own click handler skips the
                  roll: without it, following the link would also dismiss the
                  thing you were reading. */}
              <a
                className="credits-invite-link"
                href={CONTRIBUTE_URL}
                target="_blank"
                rel="noreferrer noopener"
                onClick={(event) => event.stopPropagation()}
              >
                {CONTRIBUTE_SEGMENTS.flatMap((segment, index) => (
                  index === 0 ? [segment] : [<wbr key={index} />, segment]
                ))}
              </a>
              <small>Ship a PR · Your name rolls here</small>
            </div>
          </div>

          {/* The crew, billed directly behind the invitation, so the door and
              the people who have walked through it read as one unit. Still
              ahead of every piece of match content: a contributor should not
              have to sit through anything to reach their own name. */}
          {contributors.length > 0 && (
            <section className="credits-crew" data-scale={crewScale(contributors.length)}>
              <p className="pixel-kicker">Built by</p>
              <ul>
                {contributors.map((person) => (
                  <li key={person.login}>
                    <strong>{person.name || person.login}</strong>
                    {person.name && <small>@{person.login}</small>}
                  </li>
                ))}
              </ul>
            </section>
          )}

          <Block kicker="In the red corner">
            <Name role="Lakebase Postgres" name="Databricks" note="Autoscaling OAuth · PostgreSQL 17" />
          </Block>

          <Block kicker="In the blue corner">
            {opponents.length === 0
              ? <Name role="Opponent" name="No bout completed" />
              : opponents.map((opponent) => (
                <Name key={opponent.id} role={opponent.name} name={opponent.short_name} note={opponent.edition} />
              ))}
          </Block>

          <Block kicker="The referee">
            <PixelReferee />
            <Name role="Identical task, data and client" name="One neutral verifier" />
            <Name role="One monotonic start barrier" name="Server-authoritative clocks" />
            <Name role="Commit plus nonce read-back" name="The only stop condition" />
          </Block>

          <Block kicker="No stunt doubles">
            <Name role="Application and verifier" name="FastAPI · React · psycopg 3" />
            <Name role="Owned environment" name="Terraform · AWS SDK · Databricks SDK" />
            <Name role="Neutral burst runner" name="SSM-managed m6i.large" />
            <Name role="Ring lease" name="Fenced Lakebase coordination branch" />
          </Block>

          <Block kicker="Filmed on location">
            <Name role="Live infrastructure, one owned run" name="us-west-2" />
            <Name role="Every database in this picture" name="PostgreSQL 17" />
            <Name role="Every clock in this picture" name="Measured, never simulated" />
          </Block>

          <div className="credits-rules">
            <p className="pixel-kicker">The rules</p>
            <ul>
              <li>No benchmark was harmed in the making of this demo.</li>
              <li>No opponent was denied a connection it could have made.</li>
              <li>No unverified elapsed time was invented to fill a lane.</li>
              <li>Every untimed prerequisite was disclosed on screen.</li>
              <li>Where no fair margin existed, none was claimed.</li>
            </ul>
          </div>

          {/* THE ONE AUTHORSHIP BEAT.
              This used to be two. `.credits-finale` billed the portrait and the
              name here, and the held outro then printed the same six words again
              seconds later -- the same claim twice, which is the shape behind
              "not just the Ryan Cicak show". Counting the crew card and the
              handle inside the repo URL, the roll named its author four times.
              It now names him twice: once as a contributor, once as the author.
              WHY THIS IS A CARD. Measured at 1440x900 before the change, this
              was the only beat on the roll with no container at all -- 18px of
              bare type on the arena, against a 21.6px crew name in a bordered
              plate and a fairness card more than twice its height. That is what
              "just normal text" was describing: not the face, which every beat
              inherits from .retro-screen, but the absence of the framing every
              other beat gets. So it takes the same construction as the crew,
              fairness and invitation cards, with `--steel` as its inset accent
              -- the quietest of the four, because this is authorship rather than
              the climax the roll builds toward.
              Portrait beside the name, stacking below 640px: the reasoning is
              the `.credits-finale` removal note in credits.css, which still
              holds. The rules live in credits-entry.css, NOT credits.css. Both
              sheets are hand-authored and they split the roll: credits.css
              dresses the scene and the beats that scroll through it, and
              credits-entry.css dresses this card, the held mark after the
              crawl, and the control that opens the roll. */}
          <div className="production-credit">
            <img src={brandAssets.ryanPixel} alt="" />
            <div>
              <strong>A Ryan Cicak Production</strong>
              <small>Thanks for stepping into the ring</small>
            </div>
          </div>
        </div>
      </div>

      {!rolled && !reducedMotion && (
        <button className="credits-skip" type="button" onClick={skip}>Enter · Skip roll</button>
      )}
      {/* THE HELD CARD, AND IT HOLDS THE PROJECT RATHER THAN THE AUTHOR.
          This used to print "A Ryan Cicak Production" a second time, seconds
          after the card inside the roll had said it. One of the two had to go,
          and the roll's own copy is the one that has to stay: the roll must be
          complete on its own, because under reduced motion the crawl never runs
          and this card is not shown at all. So the author credit lives in the
          roll and the last image is the game's own mark -- which is also what an
          arcade ending actually held before the attract loop came back round.
          `!reducedMotion` is a fix, not a tidy-up. `rolled` is initialised to
          reducedMotion, so this used to render immediately there and, being
          position:absolute/inset:0, it landed on top of the static list: at
          1440x900 the bars and the tiny production line struck straight through
          the "RYAN CICAK" crew card. There is no crawl under reduced motion, so
          there is no "after the crawl" for a held card to occupy. */}
      {rolled && !reducedMotion && (
        <div className="credits-outro">
          <img className="credits-outro-ring" src={brandAssets.headerRing} alt="" />
          {/* The two rules either side are the bars the production line used to
              carry. They are the one piece of that card worth keeping here. */}
          <p className="credits-outro-title">
            <span aria-hidden="true" />
            <strong>Lakebase</strong>
            <span aria-hidden="true" />
          </p>
          <em className="credits-outro-tagline">The Anti-Demo</em>
        </div>
      )}
      {/* The exit. `B · BACK` matches the app's own arcade prompt convention.
          The shared dialog lifecycle gives it initial focus and makes Escape
          follow this same path without competing with arena shortcuts. */}
      <button data-dialog-initial-focus className="game-back credits-back" type="button" onClick={exit}>B · Back</button>

      {/* The credits changed nothing about the bout underneath, so the one thing
          they owe the operator is to not look like an ending. */}
      {boutInFlight && (
        <p className="credits-live-note" role="status">Round still running · B or Esc to return</p>
      )}
    </main>
  )
}

function CreditsTitle({ tally }: { tally: CreditsTally }) {
  return (
    <div className="credits-title">
      <img className="credits-ring" src={brandAssets.headerRing} alt="" />
      <strong>Lakebase</strong>
      <em>The Anti-Demo</em>
      <p>
        {tally.bouts === 0
          ? 'No bout completed on this card'
          : `${tally.bouts} bout${tally.bouts === 1 ? '' : 's'} · Lakebase ${tally.lakebaseWins} verified win${tally.lakebaseWins === 1 ? '' : 's'}${tally.uncontested > 0 ? ` · ${tally.uncontested} uncontested` : ''}${tally.abandoned > 0 ? ` · ${tally.abandoned} abandoned` : ''}`}
      </p>
    </div>
  )
}

function Block({ kicker, children }: { kicker: string; children: ReactNode }) {
  return (
    <section className="credits-block">
      <p className="pixel-kicker">{kicker}</p>
      {children}
    </section>
  )
}

function Name({ role, name, note }: { role: string; name: string; note?: string }) {
  return (
    <div className="credits-name">
      <span>{role}</span>
      <strong>{name}</strong>
      {note && <small>{note}</small>}
    </div>
  )
}

/* ------------------------------------------------------------------ *
 * Canvas figures
 *
 * drawPixelFighter below is COPIED VERBATIM from frontend/src/App.tsx
 * (currently ~line 1225, used by renderReceiptCard). It is original
 * project art. It is duplicated here only because App.tsx does not
 * export it — see README.md for the suggested shared-module lift.
 * ------------------------------------------------------------------ */

function drawPixelFighter(
  context: CanvasRenderingContext2D,
  x: number,
  y: number,
  color: string,
  label: string,
) {
  context.fillStyle = '#fff4c2'
  context.fillRect(x + 17, y, 46, 9)
  context.fillStyle = color
  context.fillRect(x + 10, y + 12, 60, 18)
  context.fillStyle = '#dca36f'
  context.fillRect(x + 18, y + 30, 44, 32)
  context.fillStyle = '#070b22'
  context.fillRect(x + 25, y + 40, 7, 7)
  context.fillRect(x + 48, y + 40, 7, 7)
  context.fillStyle = color
  context.fillRect(x + 12, y + 64, 56, 44)
  context.fillRect(x, y + 70, 17, 24)
  context.fillRect(x + 63, y + 70, 17, 24)
  context.fillStyle = '#fff4c2'
  context.font = '400 10px "Press Start 2P", monospace'
  context.textAlign = 'center'
  context.fillText(label, x + 40, y + 79)
  context.textAlign = 'left'
}

/**
 * Original referee figure in the same flat-fillRect idiom and the same
 * 80x108 footprint as drawPixelFighter. A striped shirt and bow tie are
 * generic boxing-officiating signifiers; this is deliberately not modelled
 * on any existing game character.
 */
function drawPixelReferee(context: CanvasRenderingContext2D, x: number, y: number) {
  context.fillStyle = '#2b3350'
  context.fillRect(x + 17, y, 46, 12)
  context.fillStyle = '#dca36f'
  context.fillRect(x + 18, y + 12, 44, 34)
  context.fillStyle = '#070b22'
  context.fillRect(x + 25, y + 24, 7, 7)
  context.fillRect(x + 48, y + 24, 7, 7)
  context.fillStyle = '#e8482e'
  context.fillRect(x + 33, y + 47, 14, 8)
  for (let stripe = 0; stripe < 7; stripe += 1) {
    context.fillStyle = stripe % 2 === 0 ? '#f1ebd7' : '#101214'
    context.fillRect(x + 12 + stripe * 8, y + 56, 8, 44)
  }
  context.fillStyle = '#f1ebd7'
  context.fillRect(x, y + 62, 12, 26)
  context.fillRect(x + 68, y + 62, 12, 26)
  context.fillStyle = '#fff4c2'
  context.font = '400 10px "Press Start 2P", monospace'
  context.textAlign = 'center'
  context.fillText('REF', x + 40, y + 70)
  context.textAlign = 'left'
}

/**
 * Ref callback that paints a figure as soon as its canvas mounts.
 *
 * `draw` is a dependency rather than something stashed in a ref during render.
 * Each caller memoizes its own draw call on the values that actually change it,
 * so the ref callback is still stable and the canvas is not repainted on every
 * render of the roll.
 */
function usePixelFigure(
  draw: (context: CanvasRenderingContext2D) => void,
): (node: HTMLCanvasElement | null) => void {
  return useCallback((canvas: HTMLCanvasElement | null) => {
    if (!canvas) return
    const context = canvas.getContext('2d')
    if (!context) return
    context.imageSmoothingEnabled = false
    context.clearRect(0, 0, canvas.width, canvas.height)
    draw(context)
  }, [draw])
}

function PixelCornerFighter({
  className,
  color,
  label,
}: {
  className: string
  color: string
  label: string
}) {
  const attach = usePixelFigure(useCallback(
    (context: CanvasRenderingContext2D) => drawPixelFighter(context, 4, 6, color, label),
    [color, label],
  ))
  return (
    <canvas
      className={`credits-figure ${className}`}
      ref={attach}
      width={88}
      height={120}
      aria-hidden="true"
    />
  )
}

function PixelReferee() {
  const attach = usePixelFigure(useCallback(
    (context: CanvasRenderingContext2D) => drawPixelReferee(context, 4, 6),
    [],
  ))
  return (
    <canvas
      className="credits-figure credits-figure-ref"
      ref={attach}
      width={88}
      height={112}
      aria-hidden="true"
    />
  )
}
