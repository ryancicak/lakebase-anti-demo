/**
 * The round summary: what you ran today, one line per round.
 *
 * This is the only screen in the app whose contents outlive the server process.
 * Everything else reads the in-memory session store, which starts empty on every
 * restart; this reads `/api/receipts`, which is a directory of files. An operator
 * who runs rounds across a day and restarts the server between them sees the board
 * fill up rather than reset.
 *
 * It is deliberately sparse. A previous version of this idea was rejected for
 * showing too much, so the rule here is one result per round and nothing that
 * needs a second look. The proof screens keep the detail.
 */

import { useEffect, useState } from 'react'

import { ApiError, api } from './api/client'
import {
  type BoutReceipt,
  type LiveRound,
  type RoundResult,
  roundsWithResult,
  summariseRounds,
  summaryDuration,
  winnerLabel,
} from './recap'
import './summary.css'

function RoundRow({ result }: { result: RoundResult }) {
  const lakebase = result.lakebaseMs === null ? null : summaryDuration(result.lakebaseMs)
  const opponent = result.opponentMs === null ? null : summaryDuration(result.opponentMs)

  return (
    <li className="summary-round" data-status={result.status} data-round={result.roundId}>
      <div className="summary-round-head">
        <b>{result.roundNumber}</b>
        <strong>{result.roundTitle}</strong>
      </div>

      <div className="summary-result">
        <span className="summary-winner">{winnerLabel(result)}</span>
        {lakebase && <span className="summary-time">{lakebase}</span>}
        {opponent && result.opponent && (
          <span className="summary-against">
            {/* A lane that never finished gets a floor, never a finish time. */}
            vs {result.opponentIsLowerBound ? '>' : ''}{opponent} {result.opponent}
          </span>
        )}
      </div>

      {result.status === 'uncontested' && (
        <p className="summary-note">One lane only — a time, not a race.</p>
      )}
      {result.boutsOnRecord > 1 && (
        <p className="summary-note">
          Latest of {result.boutsOnRecord} runs.
        </p>
      )}
    </li>
  )
}

export interface SummaryProps {
  /** The round this process is running right now, if any. */
  live?: LiveRound | null
  onBack: () => void
}

export function Summary({ live = null, onBack }: SummaryProps) {
  const [receipts, setReceipts] = useState<BoutReceipt[] | null>(null)
  const [failure, setFailure] = useState<string | null>(null)
  const [attempt, setAttempt] = useState(0)

  useEffect(() => {
    let current = true
    api.receipts()
      .then((response) => {
        if (!current) return
        setReceipts(response.receipts)
        setFailure(null)
      })
      .catch((cause: unknown) => {
        if (!current) return
        // The board is on disk either way; only this read failed.
        setFailure(cause instanceof ApiError
          ? cause.message
          : 'The rounds you ran could not be read just now.')
        setReceipts([])
      })
    return () => { current = false }
  }, [attempt])

  const results = summariseRounds(receipts ?? [], live)
  const withResult = roundsWithResult(results)

  return (
    <main className="summary-screen" aria-label="The rounds you ran">
      <header className="summary-header">
        <p>Final bell</p>
        <h1>The rounds you ran</h1>
        <span>
          {receipts === null
            ? 'Reading the record…'
            : `${withResult} of ${results.length} rounds have a result.`}
        </span>
      </header>

      {failure && <p className="summary-status" role="status">{failure}</p>}

      <ol className="summary-rounds" aria-label="Result for each of the six rounds">
        {results.map((result) => <RoundRow key={result.roundId} result={result} />)}
      </ol>

      <footer className="summary-footer">
        <p>
          One honest result per round · the latest run, not the best one · this
          installation only.
        </p>
        <div className="summary-actions">
          <button type="button" onClick={onBack}>B · Fight card</button>
          <button type="button" onClick={() => setAttempt((count) => count + 1)}>
            Select · Refresh
          </button>
        </div>
      </footer>
    </main>
  )
}
