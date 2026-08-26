import { useEffect, useState, type ReactNode } from 'react'
import './round6.css'

export type RoundSixProofState = 'review' | 'running' | 'verified' | 'towelled' | 'failed'

export interface RoundSixProofProps {
  state: RoundSixProofState
  /** Authoritative server elapsed time from checkout commit to the exact Delta answer. */
  elapsedMs: number | null
  /** Authoritative server cutoff for an unfinished freshness lane. */
  censoredMs?: number | null
  /** True only after the separate checkout guardrail transaction commits. */
  separateCheckoutVerified: boolean
  competitorLabel?: string
  status?: string | null
  /** Lets App.tsx inject the existing HomeLogo without coupling this component to App.tsx. */
  homeControl?: ReactNode
  /** The shared sound toggle, injected the same way. Header-right, so it is
   *  nowhere near the bout controls and nothing here knows it is a mute. */
  soundControl?: ReactNode
  /** Existing presenter commentary or the concise verified ringside meaning. */
  ringsideContent?: ReactNode
  /** Existing navigation / explain / share controls. */
  actions?: ReactNode
}

function duration(milliseconds: number): string {
  return `${(Math.max(0, milliseconds) / 1000).toFixed(2)}s`
}

function FreshnessTimer({ elapsedMs, active }: { elapsedMs: number | null; active: boolean }) {
  const [timer, setTimer] = useState<{
    observedElapsedMs: number | null
    observedActive: boolean
    authoritativeMs: number | null
    displayMs: number
    lastTickAt: number | null
  }>(() => ({
    observedElapsedMs: elapsedMs,
    observedActive: active,
    authoritativeMs: elapsedMs ?? (active ? 0 : null),
    displayMs: elapsedMs ?? 0,
    lastTickAt: null,
  }))

  if (elapsedMs !== timer.observedElapsedMs || active !== timer.observedActive) {
    const acceptsElapsed = elapsedMs !== null && (
      timer.authoritativeMs === null || elapsedMs >= timer.authoritativeMs
    )
    setTimer({
      observedElapsedMs: elapsedMs,
      observedActive: active,
      authoritativeMs: acceptsElapsed
        ? elapsedMs
        : active && timer.authoritativeMs === null ? 0 : timer.authoritativeMs,
      displayMs: !active && elapsedMs !== null
        ? elapsedMs
        : acceptsElapsed
          ? Math.max(timer.displayMs, elapsedMs)
          : timer.displayMs,
      lastTickAt: null,
    })
  }

  useEffect(() => {
    if (!active) return
    const interval = window.setInterval(() => {
      setTimer((current) => {
        if (!current.observedActive || current.authoritativeMs === null) return current
        const now = window.performance.now()
        if (current.lastTickAt === null) return { ...current, lastTickAt: now }
        return {
          ...current,
          displayMs: current.displayMs + now - current.lastTickAt,
          lastTickAt: now,
        }
      })
    }, 32)
    return () => window.clearInterval(interval)
  }, [active])

  if (elapsedMs === null && !active) return <strong className="round6-timer-value">—</strong>
  return (
    <strong className="round6-timer-value" role="timer" aria-label="Freshness elapsed time">
      {duration(timer.displayMs)}
    </strong>
  )
}

export function RoundSixProof({
  state,
  elapsedMs,
  censoredMs = null,
  separateCheckoutVerified,
  competitorLabel = 'Aurora/RDS',
  status,
  homeControl,
  soundControl,
  ringsideContent,
  actions,
}: RoundSixProofProps) {
  const toweled = state === 'towelled'
  const verified = state === 'verified' || (toweled && elapsedMs !== null && censoredMs === null)
  const failed = state === 'failed'
  const statusLabel = state === 'review'
    ? 'UI REVIEW · NO RESULT'
    : toweled
      ? 'TOWELED · RESULT FROZEN'
      : verified
        ? 'EXACT ANSWER VERIFIED'
      : failed
        ? 'ANSWER NOT VERIFIED'
        : 'LIVE PROOF RUNNING'

  return (
    <main className="round6-screen" data-state={state}>
      <header className="round6-header">
        <div className="round6-home-slot">{homeControl}</div>
        <div>
          <p>Round 6 · OLTP → lakehouse analytics</p>
          <h1>Move live application data into the lakehouse</h1>
        </div>
        <strong>{statusLabel}</strong>
        {soundControl}
      </header>

      <section className="round6-body" aria-label="Round 6 live analytical proof">
        <aside className="round6-competitor-note">
          <strong>{competitorLabel}</strong>
          <span>requires a separate CDC stack · not built or timed</span>
        </aside>

        <div className="round6-flow" aria-label="Checkout commit to exact Delta answer">
          <article className="round6-beat round6-checkout">
            <span>1 · Checkout Postgres</span>
            <div className="round6-order-card">
              <small>Order</small>
              <strong>1 × RED-GLOVE · CHICAGO · $84.50 · COMMITTED</strong>
            </div>
          </article>

          <div className="round6-arrow" aria-hidden="true"><b>→</b></div>

          <article className="round6-beat round6-freshness">
            <span>2 · Freshness</span>
            {toweled && censoredMs !== null
              ? <strong className="round6-timer-value" aria-label="Freshness lower bound">&gt;{duration(censoredMs)}</strong>
              : <FreshnessTimer elapsedMs={elapsedMs} active={state === 'running'} />}
            <p>COMMIT → EXACT DELTA ANSWER</p>
          </article>

          <div className="round6-arrow" aria-hidden="true"><b>→</b></div>

          <article className="round6-beat round6-answer">
            <span>3 · Analytics on Delta</span>
            <div className="round6-answer-card" data-verified={verified}>
              <small>Exact answer</small>
              <strong>{verified ? '1 ORDER · $84.50 REVENUE · EXACT ✓' : 'WAITING FOR EXACT ANSWER'}</strong>
            </div>
          </article>
        </div>

        <div className="round6-guardrail" data-verified={separateCheckoutVerified}>
          {separateCheckoutVerified ? 'SEPARATE CHECKOUT COMMITTED ✓' : 'SEPARATE CHECKOUT NOT VERIFIED'}
        </div>

        {verified && (
          <p className="round6-receipt" aria-label="Round 6 receipt">
            ORDER INCLUDED ✓ · COUNT VERIFIED ✓ · SEPARATE CHECKOUT COMMITTED ✓
          </p>
        )}

        {failed && status && <p className="round6-failure" role="alert">{status}</p>}

        {ringsideContent && <div className="round6-ringside">{ringsideContent}</div>}
        {actions && <div className="round6-actions">{actions}</div>}
      </section>

      <footer className="round6-footer">
        <span>PUBLIC PREVIEW · SEPARATE DELTA HISTORY · THROUGHPUT + P99 NOT MEASURED</span>
      </footer>
      <div className="round6-scanlines" aria-hidden="true" />
    </main>
  )
}
