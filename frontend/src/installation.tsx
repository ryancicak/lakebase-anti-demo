/**
 * The installation banner: whether the infrastructure the rounds run on is
 * still there, and -- locally, and only locally -- the button that re-creates it.
 *
 * The whole screen turns on one distinction. "The account was read and the
 * resources are gone" is a fact to act on. "The account could not be read" is
 * an absence of knowledge that looks identical from a distance and means
 * something completely different. This project has three incidents from
 * conflating them, and here the expensive direction of that mistake is one
 * click from re-creating infrastructure that may already be running.
 *
 * The inversion worth stating before reading the code: a *real* sandbox sweep
 * produces `unverified`, not `verified_missing`, because the sweep deletes the
 * IAM users along with the databases and the account then cannot be read at
 * all. So the commonest real trigger for recovery is the one state that must
 * refuse to recover, and this screen's job there is to name the credential as
 * the thing to fix rather than offer a button that would spend money on a guess.
 */

import { useCallback, useEffect, useRef, useState } from 'react'

import { ApiError, api } from './api/client'
import type { InstallationStatus, RecoveryAttempt } from './api/types'
import './installation.css'

/** How often the banner re-asks. Cheap: the server answers from its own cache. */
const POLL_MS = 15_000
/** While a recovery is running, progress is worth more than politeness. */
const ATTEMPT_POLL_MS = 4_000

export interface BannerCopy {
  tone: 'gone' | 'blind' | 'unasked' | 'working'
  title: string
  /** The one sentence that must not be misread. */
  lede: string
  /** What to do instead, when there is nothing to press. */
  instead: string
  /**
   * True for the deployed app's operator note: one short line, in the demo's
   * own register, with `instead` folded behind a disclosure rather than spread
   * across the top third of the screen. See `bannerCopy`.
   */
  quiet?: boolean
}

/**
 * The four states as four different sentences.
 *
 * Pure, and exported, so the wording can be asserted without rendering: the
 * thing being protected is the words themselves, not the markup.
 *
 * Two audiences and two contexts, resolved here so the component below stays a
 * renderer:
 *
 * **A viewer gets nothing.** Not because the fault is unreal, but because none
 * of it is theirs: every remedy this surface names is a shell command on the
 * owner's machine, and this banner is fixed to the top of a screen a presenter
 * projects. The server has already emptied the prose; this is the second half
 * of the same decision. What a viewer actually needs -- which rounds can run
 * tonight -- is the round-select screen's answer, where it has always been.
 *
 * **The deployed operator gets it quietly.** Recovery is physically impossible
 * in the deployed app, so nothing here is one click from being acted on and
 * there is nothing to put in front of the title card. The diagnosis is not
 * deleted, it is folded: one line saying what was checked, and the server's own
 * words one click away.
 *
 * **A local operator is unchanged.** That is the machine that holds the
 * Terraform state, the only context where this advice can be followed, and the
 * path every bout to date has been run on.
 */
export function bannerCopy(status: InstallationStatus): BannerCopy | null {
  // `=== 'viewer'` and never `!== 'operator'`: an absent field is a server that
  // predates this distinction, and it must fall back to showing the panel.
  if (status.audience === 'viewer') return null
  const copy = presenceCopy(status)
  if (!copy || !status.deployed || copy.tone === 'working') return copy
  // The deployed app has no credentials to fix and no installer to run, so the
  // local advice below would send its reader to a shell that does not exist
  // here. It gets the physical reason instead, which is the server's own words.
  return {
    ...copy,
    title: 'BACKSTAGE · FOR THE OPERATOR',
    lede: deployedLede(status),
    instead: status.recovery.refusal || copy.instead,
    quiet: true,
  }
}

/**
 * One line for the deployed operator: what was checked, and where the audience's
 * question is answered.
 *
 * It reports this surface's own reading and stops. It deliberately does not say
 * which rounds can run -- that is `/api/catalog`'s verdict, rendered on the
 * round-select screen, and a second surface restating it is exactly how the two
 * come to disagree.
 */
function deployedLede(status: InstallationStatus): string {
  const card = 'Which rounds can run tonight is decided on the round-select screen, not here.'
  switch (status.state) {
    case 'verified_missing':
      return `The AWS account was read and ${status.absent_resources} of `
        + `${status.sealed_resources} sealed resources are absent from it. ${card}`
    case 'unverified':
      return 'This app could not read the AWS account, so it can say nothing either way '
        + `about the sealed infrastructure. ${card}`
    default:
      return `Nothing has read the AWS account in this process yet. ${card}`
  }
}

function presenceCopy(status: InstallationStatus): BannerCopy | null {
  if (status.attempt && (status.attempt.phase === 'spawned' || status.attempt.phase === 'running')) {
    return {
      tone: 'working',
      title: 'RE-CREATING THE INFRASTRUCTURE',
      lede: status.attempt.detail,
      instead: 'Leave this open. It keeps polling, and it survives a reload.',
    }
  }
  switch (status.state) {
    case 'verified_missing':
      return {
        tone: 'gone',
        title: 'THE SEALED AWS INFRASTRUCTURE IS GONE',
        lede:
          `The account was read and ${status.absent_resources} of `
          + `${status.sealed_resources} sealed resources are absent from it. `
          + 'Every round that connects to Aurora or RDS will fail until they are '
          + 're-created.',
        instead: status.recovery.offered
          ? 'Read what this would create, then confirm it below.'
          : status.recovery.refusal,
      }
    case 'unverified':
      return {
        tone: 'blind',
        title: 'THE ACCOUNT COULD NOT BE READ',
        // The sentence the whole feature exists for.
        lede:
          'This is NOT a report that anything is missing. '
          + `${status.reason || 'The sweep failed'}, so the `
          + `${status.sealed_resources} sealed resources are neither confirmed `
          + 'present nor confirmed gone.',
        instead:
          'Fix the read, not the infrastructure. Usually a lapsed SSO session:\n'
          + '\n'
          + '    aws sso login --sso-session databricks-sandbox\n'
          + '\n'
          + 'This banner answers itself within about 30 seconds of the credentials '
          + 'coming back, with no restart.\n'
          + '\n'
          + 'One thing to know before reaching for a workaround: a sandbox sweep '
          + 'deletes the IAM users as well as the databases, so a real reap also '
          + 'lands here rather than on a confirmed absence. That makes this the '
          + 'likeliest way a genuine loss shows up — and it still is not proof of '
          + 'one. If the resources really are gone, the first sweep that succeeds '
          + 'will say so and the recovery control will appear.',
      }
    case 'never_checked':
      return {
        tone: 'unasked',
        title: 'NOBODY HAS LOOKED YET',
        lede:
          'The account has not been read in this process, so nothing here knows '
          + 'whether the sealed resources exist. This is NOT a report that '
          + 'anything is missing, and it is NOT a report that anything is fine.',
        instead: 'Press “Check the account now”. It takes a few seconds.',
      }
    default:
      // Read, and everything is there. A healthy installation does not get a
      // banner over a demo that is being projected in front of customers.
      return null
  }
}

function Progress({ attempt }: { attempt: RecoveryAttempt }) {
  return (
    <div className="installation-progress">
      <p className="installation-phase" data-phase={attempt.phase}>
        {attempt.phase === 'succeeded' && 'The installer finished with exit 0.'}
        {attempt.phase === 'failed' && `The installer exited ${attempt.exit_code ?? '?'}.`}
        {attempt.phase === 'lost' && 'The installer is gone and never recorded an ending.'}
        {(attempt.phase === 'running' || attempt.phase === 'spawned') && 'Running…'}
      </p>
      <p className="installation-detail">{attempt.detail}</p>
      {attempt.log_tail.length > 0 && (
        <pre className="installation-log" aria-label="Recovery log">
          {attempt.log_tail.join('\n')}
        </pre>
      )}
    </div>
  )
}

/**
 * The confirmation. Two deliberate acts, not one click.
 *
 * The first press only reveals what would happen; the phrase that authorises
 * the spend is issued by the server, names the generation and states the daily
 * cost, and has to be typed. That is the same shape as
 * `cleanup --force-round6`, which makes an operator type the environment's own
 * token, and it is why this cannot be defaulted by a client that never showed
 * the operator either number.
 */
function Confirm({ status, onDone }: { status: InstallationStatus; onDone: () => void }) {
  const [open, setOpen] = useState(false)
  const [typed, setTyped] = useState('')
  const [pending, setPending] = useState(false)
  const [failure, setFailure] = useState<string | null>(null)
  const phrase = status.recovery.confirmation_phrase

  if (!open) {
    return (
      <button type="button" className="installation-reveal" onClick={() => setOpen(true)}>
        Show what recovery would do
      </button>
    )
  }

  return (
    <form
      className="installation-confirm"
      onSubmit={(event) => {
        event.preventDefault()
        if (typed.trim() !== phrase || pending) return
        setPending(true)
        setFailure(null)
        api.recoverInstallation(typed.trim())
          .then(onDone)
          .catch((cause: unknown) => {
            setFailure(cause instanceof ApiError
              ? cause.message
              : 'The recovery could not be started.')
          })
          .finally(() => setPending(false))
      }}
    >
      <p className="installation-plan">{status.recovery.plan}</p>
      <p className="installation-cost">
        Keeping this installation alive costs about ${status.recovery.usd_per_day} a day.
        <small>{status.recovery.usd_per_day_basis}</small>
      </p>
      <p className="installation-budget">
        {status.recovery.attempts_in_window} of {status.recovery.attempts_allowed} recoveries
        used in the last 24 hours. The limit is on disk, so restarting the server does not
        reset it.
      </p>
      <label htmlFor="installation-phrase">
        Type this exactly to confirm: <code>{phrase}</code>
      </label>
      <input
        id="installation-phrase"
        name="confirm"
        autoComplete="off"
        value={typed}
        onChange={(event) => setTyped(event.target.value)}
      />
      {failure && <p className="installation-failure" role="alert">{failure}</p>}
      <div className="installation-actions">
        <button type="button" onClick={() => { setOpen(false); setTyped('') }}>Cancel</button>
        <button type="submit" disabled={pending || typed.trim() !== phrase}>
          {pending ? 'Starting…' : 'Re-create the infrastructure'}
        </button>
      </div>
    </form>
  )
}

export function InstallationBanner() {
  const [status, setStatus] = useState<InstallationStatus | null>(null)
  const [checking, setChecking] = useState(false)
  const [readFailure, setReadFailure] = useState<string | null>(null)
  const mounted = useRef(true)

  const read = useCallback((recheck: boolean) => {
    if (recheck) setChecking(true)
    return api.installation(recheck)
      .then((next) => {
        if (!mounted.current) return
        setStatus(next)
        setReadFailure(null)
      })
      .catch((cause: unknown) => {
        if (!mounted.current) return
        // A banner that cannot read its own endpoint says so. It must not fall
        // back to silence, which would read as "everything is fine".
        setReadFailure(cause instanceof ApiError
          ? cause.message
          : 'The installation status could not be read.')
      })
      .finally(() => { if (mounted.current) setChecking(false) })
  }, [])

  useEffect(() => {
    mounted.current = true
    queueMicrotask(() => { if (mounted.current) void read(false) })
    return () => { mounted.current = false }
  }, [read])

  const running = Boolean(
    status?.attempt && (status.attempt.phase === 'spawned' || status.attempt.phase === 'running'),
  )

  useEffect(() => {
    const timer = window.setInterval(() => { void read(false) }, running ? ATTEMPT_POLL_MS : POLL_MS)
    return () => window.clearInterval(timer)
  }, [read, running])

  if (readFailure) {
    return (
      <aside className="installation-banner" data-tone="blind" role="status">
        <h2>THE INSTALLATION STATUS COULD NOT BE READ</h2>
        <p>{readFailure}</p>
      </aside>
    )
  }
  if (!status) return null
  const copy = bannerCopy(status)
  if (!copy) return null

  // Everything below the lede, as one piece, so the quiet variant folds exactly
  // the same content rather than a shortened copy of it that could drift.
  const diagnosis = (
    <>
      <p className="installation-instead">{copy.instead}</p>

      {status.transitional_recovery && (
        <p className="installation-transitional">{status.transitional_recovery}</p>
      )}
      {status.mutation_in_progress && (
        <p className="installation-holder">{status.mutation_holder}</p>
      )}

      {status.attempt && <Progress attempt={status.attempt} />}

      <div className="installation-controls">
        <button type="button" onClick={() => { void read(true) }} disabled={checking}>
          {checking ? 'Reading the account…' : 'Check the account now'}
        </button>
        {/* `running` is checked here as well as on the server. The server's
            single-flight guard is the real one; this stops the control being
            drawn at all while an installer is in flight, so nobody presses a
            button whose only outcome would be a refusal and a corpse in the log. */}
        {status.state === 'verified_missing' && status.recovery.offered && !running && (
          <Confirm status={status} onDone={() => { void read(false) }} />
        )}
      </div>

      {status.checked && (
        <p className="installation-age">
          Read {status.checked_seconds_ago}s ago.
          {status.deployed && ' This is the deployed app: it can report, and nothing else.'}
        </p>
      )}
    </>
  )

  return (
    <aside
      className="installation-banner"
      data-tone={copy.tone}
      data-quiet={copy.quiet ? 'true' : undefined}
      data-state={status.state}
      role="status"
    >
      <h2>{copy.title}</h2>
      <p className="installation-lede">{copy.lede}</p>
      {copy.quiet ? (
        <details className="installation-diagnosis">
          <summary>Show the operator diagnosis</summary>
          {diagnosis}
        </details>
      ) : diagnosis}
    </aside>
  )
}
