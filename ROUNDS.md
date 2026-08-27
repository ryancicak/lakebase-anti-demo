# Lakebase: The Anti-Demo

## Acceptance and build specification

**Category:** The persona-aware competitive proof engine.

**Promise:** Name the competitor. Read the room. Ring the bell. The same task, data, client, transaction, and verifier run against Lakebase and one real AWS PostgreSQL competitor. The result is one verified number, not a dashboard.

## Executable contest path

Round 1 is **Wake this idle app**, with two honest matchup modes:

- **Aurora Serverless v2:** both systems are independently verified at scale zero. The same unique PostgreSQL transaction runs through the same client and retry policy. Each clock stops only after commit and nonce read-back verification.
- **RDS PostgreSQL:** the runner first describes the selected real RDS PostgreSQL instance through the AWS control plane. RDS stays in the right lane as `NO SCALE-TO-ZERO` because it has no automatic connection-triggered scale-to-zero/wake state. It receives no connection attempt, elapsed time, or fake failure. Lakebase wins only after its own transaction commits and verifies.

If Lakebase does not verify in the RDS matchup, the result is `NO WINNER DECLARED`. Manual RDS stop/start is not substituted into the round because an application connection cannot perform that wake.

Round 2 is **Make this schema change safely**:

- Both lanes start from the same seeded source contract.
- Lakebase creates a native branch, Aurora creates a copy-on-write point-in-time clone, and RDS creates a point-in-time restore.
- Each isolated environment receives the identical `orders.delivery_instructions` migration and nonce-bearing application transaction.
- A lane verifies only after reading the new value back and proving the source schema and source data remain unchanged.
- Re-do deletes only deterministic, ownership-verified isolated artifacts. It never waits for the sources to become idle.

Round 3 is **Recover this deleted order**:

- Interactive arm commits the same exact run-owned row to Lakebase and the selected competitor, or accepts that exact row if already present. It rejects a mismatched payload, confirms that no recovery artifact exists, and waits only long enough for the row to predate a full provider-clock second. It does not pre-wait recovery eligibility or capture a recovery timestamp.
- The bell publishes one common monotonic deletion barrier. Each lane deletes the exact row and captures `recovery_at = floor(observed_at) - 1 second` in the same PostgreSQL statement; its timer includes eligibility waiting, restore/readiness, TLS, and both verified reads.
- Lakebase requests a branch at `spec.source_branch_time`; Aurora requests a full-copy PITR recovery cluster; RDS requests a PITR restore. AWS requests use explicit `RestoreToTime`; Round 3 does not request Aurora copy-on-write.
- A lane closes its deletion session after commit, then stops only after a fresh recovery connection reads the exact recovered order and a fresh source connection still proves it absent. READY/AVAILABLE never stops a clock.
- Fairness is explicit: same exact row, one deletion barrier, eligibility + recovery + verified read timed, and the source remains deleted.
- Re-do deletes only owned recovery artifacts and reconciles only the exact synthetic row.
- In either matchup, **Throw in the towel** is eligible only while Round 3 is running, Lakebase has verified, and the selected opponent is still active. Accepting it freezes the authoritative cutoff and presents that opponent only as a censored lower bound, for example `>90.00s`; no unverified elapsed time is invented.
- The towel stops only unfinished recovery work and preserves Lakebase's verified result. In-flight provider mutations must settle before the normal ownership-scoped recovery cleanup removes artifacts and reconciles the exact synthetic row.
- A cleanup failure is not a terminal result: the fenced bout remains open and exposes **Retry cleanup**. **Next** remains unavailable until cleanup succeeds and the session reaches terminal `TOWELLED`.
- The final receipt has the shape `TOWEL THROWN AT <cutoff>s · LAKEBASE VERIFIED <elapsed>s · AURORA UNVERIFIED WHEN STOPPED · LOWER BOUND`, with Aurora's lane retained as a censored lower bound of the presenter's cutoff. The placeholders stand for whatever that bout measures: this is the required format, not a quoted result, and no towel has been thrown in a live bout. LinkedIn copy distinguishes the exact presenter cutoff from the censored opponent result.

Round 4 is **Move lakehouse data into live applications**:

- The capability story is reverse ETL / OLAP → OLTP: governed Analytics Delta → managed reverse ETL → operational Postgres → a live application in one Databricks platform path, without a separate reverse-ETL product or operating stack. This does not claim zero configuration or zero security work.
- This bout uses one exact customer risk-score row as the concrete example and verifies the Lakebase Postgres destination through a fresh application read. The pattern also applies to segments, recommendations, fraud flags, pricing, and inventory.
- The result names the exact lakehouse change that reached the live app. Delta version, full nonce, timings, and exact-row identity remain its proof receipt.
- RDS/Aurora alone are OLTP sinks and do not move lakehouse data. The same outcome requires an added stack for source/target connectors, IAM/secrets, network access, mappings/upserts, checkpoints/retries, and monitoring. That stack is not built or timed, so no cross-platform speed margin is claimed.
- Re-do changes this bout's score from v1 to v2 in the lakehouse, then verifies the same customer record updates in the live app.

Round 5 is **Get spike-ready** (the capability under test is surviving a connection spike; the
round is named for the outcome it actually scores, which is readiness setup):

- This is a two-phase proof. Phase 1 is the primary score: both setup workflows share one monotonic T0, launch within 10 ms, and run under one 30-minute deadline. Each lane stops only at its exact public setup gate. The UI never adds setup elapsed time to burst p99.
- Lakebase uses its returned `read_write_pooled_host`: 0 separately provisioned per-bout pooling components and 0 per-bout pooling infrastructure mutations. Its baseline still discloses native-login, ordinary-role, and runner-credential preparation.
- The selected Aurora/RDS lane performs 9 timed, journaled competitor mutations: 1 per-bout Proxy security group, 1 default-egress change, 4 exact security-group rules, 1 RDS Proxy, 1 target-group configuration, and 1 target registration. Its setup clock stops at the exact application transaction. The Proxy is configured for 90% maximum connections and a 120-second connection-borrow timeout.
- IAM service role, runner permission, and dedicated proxy credential secret(s) are sealed install-time prerequisites outside the setup clock. They are required configuration, not timed mutations. The AWS design still adds RDS Proxy, Secrets Manager, IAM, and network configuration; RDS Proxy and Secrets Manager are incremental billable services.
- Phase 2 is a warm burst that validates the setup result. One neutral, SSM-managed `m6i.large` runner uses Python 3.12, psycopg 3.3.4, prepared statements disabled, and TLS `verify-full` with the same explicit `sslrootcert` in both lanes. Each lane gets four warmups and 128 attempts with exactly 64 maximum concurrent attempts and no more than 10 ms burst-launch skew.
- Every fresh burst connection uses a parameterized `SELECT` for one run-unique probe UUID/value, returns the exact response plus `pg_backend_pid()`, and commits. Successful raw latencies produce nearest-rank p99. Those counts and p99 values are secondary evidence, never the primary score and never part of the setup margin.
- The downstream witness holds 64 clients per lane and uses a direct `pg_stat_activity` observer. All clients must verify; both unique returned backend PIDs and peak observed sessions must remain below 64. The direct RDS path is limited to observation and run-owned cleanup; all tested application connections use the new per-bout Proxy.
- A winner and setup margin appear only when both primary setup lanes and every burst, witness, fairness, exact-transaction, and cleanup gate validate. Setup failure or a setup towel displays no winner and no margin.
- Cleanup closes clients, deletes only run-owned probes and the 9 journaled competitor mutations, verifies a clean baseline, and releases the runner flock. **Ring Again** creates a new bout after that clean baseline and never reuses the Proxy.
- If Round 5 cleanup fails, the failed bout remains fenced with no winner or margin. The UI exposes only idempotent **Retry Cleanup**—never Ring Again or continuation—until the retry verifies the clean baseline and releases the bout.
- Either selected AWS engine requires the added RDS Proxy, Secrets Manager, IAM, and network configuration described above; only the selected lane is executed and scored.

Round 6 is **Move live application data into the lakehouse**:

- The capability story is OLTP → lakehouse analytics: operational Lakebase Postgres → built-in change feed (CDF) → separate Delta history → an exact analytical answer.
- One committed checkout row is the concrete proof, not the name of the broader capability. The clock stops only when that exact row appears once in Delta and produces the expected answer.
- A separate checkout must also commit successfully as a correctness guardrail. This does not claim measured throughput, p99 impact, or zero production impact.
- Aurora/RDS require a separately selected, secured, operated, and priced CDC-to-Delta stack. That stack is not built or timed, so no AWS speed margin or dollar savings is claimed.

Round 1 re-do is a different contract: both clocks start together and independently freeze only at genuine idle/scale-zero. The setup uses each product's shortest native automatic suspension timeout—60 seconds for Lakebase and 300 seconds for Aurora Serverless v2—rather than making Lakebase wait five minutes for cosmetic parity. RDS is explicitly `NO SCALE-TO-ZERO`, not falsely reported as idle.

## Experience contract

Backstage setup may select:

1. One competitor: Aurora Serverless v2 or RDS PostgreSQL.
2. Exactly one primary persona and at most two secondary personas.
3. One to three customer priorities: cost, simplicity, and/or performance.
4. One recommended round, with an explicit operator override before arming.

The retro title, matchup, ringside personas, and bell are a short setup ritual. The proof never leaves that world: ringing the bell cuts into one 8-bit ring with pixel database fighters and equally weighted red and blue corners. Once timing begins, the audience sees only:

- one task name;
- two equally weighted product lanes, with a timer only for each eligible participant;
- one final fairness line and one remembered result;
- in Round 5 only, a clearly separated primary setup panel, secondary burst validation panel, and compact required component disclosure.

No chat, charts, unrelated infrastructure inventory, health bars, cumulative score, fake progress, or scrolling UI appears in the measured proof.

Setup progress is browser-native. Back and Forward restore the prior screen, while opponent, priorities, personas, round override, sound, and the last safe setup location survive a reload. Armed, running, and completed sessions are never revived from browser storage; a reload from a live stage returns safely to the fight card and requires a fresh arm.

## Persona contract

Persona selection changes discovery order, vocabulary, recommended round, and the presenter's role-specific talk track. It never changes the task, verifier, timestamps, receipt, or result.

**Explain to the room** is a minimal, audience-facing briefing that can be projected or shared:

1. `FOR THE <ROLE>` with a compact text-only role switcher and plain `COST`, `SIMPLICITY`, and `PERFORMANCE` context chips.
2. **WHAT THIS MEANS** — one plain-language authored sentence, at most 25 words.
3. **QUESTION FOR THE ROOM** — one natural invitation to discuss, at most 18 words and exactly one question mark.
4. **WHAT WE PROVED** — one exact, comprehensible receipt-backed line, normally at most 26 words. Round 5 may use up to 36 words because the new-Proxy setup boundary and the untested existing-Proxy caveat must both remain visible.
5. `B · BACK TO THE RING`.

The ten runtime roles, six rounds, and seven canonical non-empty priority sets resolve all 420 tracks from one reviewed JSONL corpus. Every record preserves its stable meaning and question IDs, exact final text, and KEEP/REWRITE decisions. A separate 23-record JSONL outcome matrix supplies proof templates and the override copy for partial, failed, towel, and no-result states. The frontend parses those two static sources once into typed lookups and applies one round-specific evidence classifier; no second handwritten corpus or generated copy layer exists.

Pair and triple selections use a complete integrative question, not a concatenation of single-priority clauses. A missing compiled leaf is an error. The proof line is invariant across role and priority for the same session; only exact receipt tokens such as elapsed times, competitor, verified row ID, or score may vary. If neither lane has verified evidence, the resolver uses a round-specific no-result meaning and question across every role and priority. A towel in that state reports the actual cutoff and any censored lower bounds instead of exposing success-authored language.

Every track is dual-use: a presenter can read it verbatim without sounding like backstage instruction, and an attendee can read it silently and understand what happened and why it may matter. Visible copy never exposes template machinery or labels people as a persona, consumer, or beneficiary. Relevance is framed for the selected role and tested through the room question rather than asserted as fact. Lower bounds and material caveats are explained in plain language inside **WHAT WE PROVED**.

The dialog renders no priority cards, accordions, nested details, success measure, universal disclosure stack, or supporting priority list. Material caveats live directly in **WHAT WE PROVED**. Extended evidence remains available at its dedicated destination: configured capacity and Round 5's full setup/burst proof in **Instant Replay**; descent, bout, standing, and pricing-receipt evidence in **What It Cost**; exact calls in **Technical Details** or replay steps; and the portable exact result in **Share Receipt**.

Cost language distinguishes measured rates, standing or per-event spend, and measured wait. Operator toil remains unmeasured unless executed steps are counted; cost of delay remains a discovery question. Performance names the exact executed stop boundary. Simplicity names concrete tools, tickets, runbooks, approvals, steps, handoffs, and owners without treating demo automation as proof of customer simplicity. A new acceptance condition requires a new round and explicit re-arm.

The source registry is `config/personas.json`. Authored Tiger Team content remains distinguishable from Anti-Demo drafts.

Each persona carries a `pain` line: one short, in-voice sentence naming the problem that persona lives
with. It is the only presenter copy rendered *before* selection — the roster card shows portrait,
nickname, role, and `pain`, so an audience member can recognise themselves at pick time rather than one
screen later. A `pain` line names a pain; it must not promise a capability no round demonstrates.

## Copy contract

One canonical phrase per concept. `VERIFIED` is a technical state, not a slogan; the two must never
be substituted for each other.

| Concept | Canonical phrase | Where it may appear |
| --- | --- | --- |
| Banner promise | `SAME TASK · SAME DATA · ONE HONEST RESULT` | Title art and title screen only. Never on a receipt, verdict, or lane. |
| Fair-start contract | `FAIR-START CONTRACT · <round's own enumeration>` | Receipt footer, fairness line, recap page. Always label-prefixed so it reads as a contract, not a competing promise. |
| Lane proof state | `VERIFIED` · `COULD NOT VERIFY` · `UNVERIFIED WHEN STOPPED · LOWER BOUND` · `NOT SUPPORTED · N/A` | Per-lane status and per-lane receipt value only. |
| Round outcome | `RESULT DECLARED` · `STOPPED SHORT` · `NO WINNER DECLARED` · `NO RESULT DECLARED` | Verdict band, scorecard, recap exit label. |
| Provenance | `ONE LIVE RUN · NOT A BENCHMARK` plus the receipt ID | Receipt fine print, share copy, recap footer. |
| Idle floor is billed | `FLOOR IS BILLED · NOT FREE ON EITHER SIDE` | Round 1's return-to-idle screen only, directly under the idle-policy floor line. Never abbreviated to a claim that Lakebase idles free — it does not; Lakebase bills its 60s exactly as Aurora bills its 300s, and the only claim on this axis is the 5x ratio. Dollars-per-descent and their derivations stay behind "Explain to the room". |
| Round not runnable here | `NOT EXECUTABLE HERE · NO SEALED CONTRACT · NO TIMED RESULT` | A round whose sealed manifest contract is absent. Says *seal*, not *verifier*: Round 6 ships a verifier in every installation, while its live-validated v6 seal is a per-installation fact — a Round 4 re-seal that changes the sealed Round 4 identity drops it, and the round stops arming until setup re-seals it. |
| Databricks feature maturity | `PUBLIC PREVIEW` | The upstream product's own maturity only (Round 6 CDF). Never a statement about whether a round can run. |

Five variants are banned because each said something its own artifact could not support: a proof
state standing in for the banner promise, a lane status standing in for a receipt verdict, a round
title naming a result its contract refuses to score, a bare lane label that omits *why* there is no
time, and one word doing duty for both upstream feature maturity and this deployment's inability to
run a round.

The table above is the definition of record, and it is prose rather than a build gate. Nothing
in this repository scans for those five variants, and nothing fails a build on them: CI runs
four jobs — `hygiene`, `python`, `bootstrap` and `frontend` — and none of them reads copy. A
`check_copy.py` scan does exist, but it lives in a sibling scratch directory on the author's
machine, outside this repository, so a reader who clones this tree cannot run it; treat that
check as unavailable and read the table by hand. A deliberate exception ends the offending line
with `copy-audit: allow <rule-id>` — the marker that scan honours — and must update the table
above regardless, because the table is the part a reader actually has.

## Runtime architecture

```text
React audience/presenter UI
        -> FastAPI orchestrator
             -> dedicated Lakebase fenced lease (coordination only)
             -> Rounds 1-4 neutral async PostgreSQL client
                  -> Lakebase Autoscaling
                  -> Aurora Serverless v2 or RDS PostgreSQL
             -> Round 5 least-privilege STS role -> SSM
                  -> neutral m6i.large EC2 runner
                       -> Lakebase built-in pooled host
                       -> fresh per-bout RDS Proxy -> RDS PostgreSQL
                       -> RDS PostgreSQL direct (untimed observer/cleanup only)
```

The orchestrator remains the authority in both local and deployed operation. Both Round 5 lanes originate from the same sealed runner, and database credentials never return to the app or browser.

For **deployed** Round 5 the app does not assume the execution role directly, and the difference is what makes the round reachable from a container at all. The execution role's trust policy names exactly one principal and is sealed at first provision, so an app authenticating as itself can never be that principal. Instead an installation may seal a shared `anti-demo-runtime` role that both the operator's principal and the app's service principal are trusted to assume, and the execution role is then sealed to trust *the runtime role*. The deployed path is therefore **two STS hops**: ambient app credentials assume the runtime role, and the runtime role assumes the sealed execution role. `sts:GetCallerIdentity` returns the same sealed identity from the laptop and from the app, which is the property the seal actually checks. The shared role is created only when `ANTI_DEMO_RUNTIME_PRINCIPAL_ARNS` is set at first provision; it defaults to unset, and because the trust policy is sealed at first provision, adding it later is a fresh install rather than a repair.

One fenced lease in a separate Lakebase coordination endpoint serializes infrastructure-changing
work across users and app replicas. Databricks SSO owns the lease. Waiting users can inspect the
operator name/email, round, opponent, phase, heartbeat, and expiry; internal lease and fencing
tokens never enter browser payloads. `ARMED -> RUN_COMMITTED` is one atomic database update.

Production code has no simulation mode. Deterministic fakes exist only inside automated tests. UI preview states never display invented measurements.

## Technical integrity

- Monotonic server timing; ticking browser clocks are presentation only and are replaced by authoritative server measurements at verification.
- One start barrier with recorded launch skew.
- Same PostgreSQL major version where practical, schema, seed, TLS posture, SQL, nonce, and retry policy for timed participants.
- Fresh connections; pools and health checks are sealed before a cold-wake run.
- Timed database endpoints must resolve to the same cloud region; arming fails on a mismatch.
- Compute is capacity-matched on memory, and the configured sizes are disclosed on screen behind "More details" on every round. Lakebase runs `0.5–2 CU` (~1–4 GB), Aurora Serverless v2 runs `0–2 ACU` (~0–4 GiB), and RDS PostgreSQL runs `db.t4g.medium` (2 vCPU / 4 GiB) — one 4 GB/GiB ceiling on all three, single node and no HA on any side. A `capacity_parity` check fails `doctor` if either side drifts off that ceiling or below the 128-client Round 5 contract. These are deliberately *not* vendor defaults; each side's auto-suspend interval is instead that vendor's shortest supported value (Lakebase 60s, Aurora 300s, which is AWS's documented minimum, and provisioned RDS has no automatic idle pause at all).
- Each vendor's idle floor is also *priced*, not just disclosed, and neither side's floor is free. Round 1 shows one sentence — the floor is billed on both engines, Aurora's is 5x longer, and it is charged per descent rather than per day — with the arithmetic behind "Explain to the room". Per descent, Lakebase bills `$0.00046–$0.00185` (0.5–2 CU × 0.213 DBU/CU-hour × 60s ÷ 3600 × $0.26/DBU) and Aurora bills at most `$0.02` (2 ACU × $0.12/ACU-hour × 300s ÷ 3600). Aurora's figure is an **upper bound, not an estimate, and now a sampled one**. It does *not* decay ACU during the descent: CloudWatch caught two real descents (a sample taken in the author's own account; the raw CloudWatch note is a local run artifact and is not published with this repository, so treat the two dollar figures below as one installation's observation rather than a representative rate) and both stepped straight to a dead-flat `0.500 ACU` — a quarter of the ceiling — held it with zero connections for the whole way down, then went to zero in one move, costing `$0.005363` and `$0.009508`. So `2 ACU × 300s` overstates because of a measured plateau, not because of an unobserved curve; at a `2 ACU` plateau the same arithmetic would be exactly met rather than conservative, which is the reading the retired "decay" wording was hiding. Two measured things the figure does not cover: the writer first holds its *bout* plateau (~`1.5 ACU`; `2.00` only transiently) after the app disconnects — 172s on one Round 1 return-to-idle wait — before the floor's clock starts at all, and those two descents ran 5 and 15 minutes against the same 300s product floor, so 300s is AWS's documented minimum rather than an observed duration. The low bound stays `$0` because Aurora's minimum capacity is 0 ACU and every parked bucket reads bit-exact zero, so no claim is made that Aurora burns compute indefinitely while idle — but a running Serverless v2 writer cannot report below `0.5 ACU`, so an actual descent cannot bill less than `$0.005`, and the on-screen band reason says so beside the band. Provisioned RDS carries the unconditional case — it cannot descend at all, so it bills every hour of every day. Compute only; storage bills on all three regardless of idle state and is a separate line. `server/descent_cost.py` derives every figure from `server/capacity.py` and `server/cost_model.py`, so a resize or a price change moves the box, the disclosure and the arithmetic together.
- Secrets never enter browser payloads, logs, Terraform output, or committed files.
- Coordination traffic never touches or wakes the measured Round 1 Lakebase endpoint.
- Every live phase has a bounded lease; active work heartbeats it and abandoned work expires.
- Aurora ingress is restricted to a sealed allow-list; never `0.0.0.0/0`.
- Aurora and direct public RDS ingress admit the operator's exact current `/32`, plus — on an installation that seals them — the four Databricks-published serverless egress prefixes for the workspace's region, which is what lets the deployed app race an opponent over TCP 5432 at all. Both sets are sealed into the manifest and applied to the database security groups by Terraform; neither permits broad CIDR ingress, and the egress prefixes are the vendor's published ranges rather than anything this project chooses. An installation that seals no egress prefixes admits the operator `/32` alone, and its deployed app is refused the four AWS-backed rounds at the network. RDS separately accepts scored PostgreSQL traffic from the owned proxy security group and direct observer/cleanup control traffic from the owned runner security group.
- Every baseline resource is tagged and persisted in the manifest. Per-bout resources are ownership-tagged and tracked only in the server-side cleanup journal; cleanup remains ownership-scoped and dry-run first, and journal details never enter browser payloads.
- Interrupted provisioning resumes from the same manifest and revalidates both identities before changing resources.
- Round 5 cannot arm without a complete secret-free clean-baseline manifest v5 sealing the runner baseline, exact app-assumed execution role, SSM document and harness digests, returned Lakebase hosts, install-time IAM/credential prerequisites, and every frozen proof constant. Earlier manifests do not prove the clean baseline. Per-bout security-group/rule, Proxy, target-group, and target identifiers remain internal and never enter browser payloads.
- Dedicated proxy credential secret(s) are populated and sealed at install time, outside the bout clock; credential values never enter Terraform state, the manifest, logs, browser payloads, or command output.
- The Round 5 app-assumed role can orchestrate only the sealed runner and ownership-scoped per-bout setup workflow. It cannot read database secrets. The runner permission is sealed at install time and permits access only to the dedicated proxy credential secret(s).
- Both Round 5 lanes must use the same sealed combined trust-bundle path and SHA-256 as `sslrootcert` with TLS `verify-full`; trust material is installed before readiness and cannot be downloaded during the proof.

## Definition of done

- Ten consecutive successful end-to-end Aurora rehearsals.
- Three consecutive `reset -> arm -> run` cycles without manual intervention.
- `doctor` detects expired auth, changed egress IP, missing resources, nonzero compute, open connections, and bad seed state.
- Every cloud wait is bounded and produces a useful error.
- A failed lane stays visibly failed.
- The RDS capability lane stays visibly untimed and never displays `0.00`.
- Round 1 re-do stops each clock only at independently confirmed idle/scale-zero.
- Round 2 re-do stops each clock only after its owned isolated environment is confirmed deleted.
- Round 3 re-do stops each clock only after its owned recovery environment is confirmed deleted.
- Ring Again repeats the exact completed matchup only after cleanup proves a clean baseline; it creates a new bout and never reuses the prior Proxy, even if browser history or setup selections changed.
- No secrets in UI, logs, committed files, or test snapshots.
- Audience view works at 1920x1080 without scrolling.
- Cleanup confirms no demo-owned AWS or Databricks resources remain.
- Round 5 declares a setup winner and margin only when both setup lanes, both 128-attempt result sets, both launch-skew gates, both 64-client witnesses, exact transactions, and run-owned cleanup evidence verify under the frozen contract.

## Explicitly out of scope for V1

- GCP, Azure, DynamoDB, Aurora DSQL, and self-managed PostgreSQL.
- Runtime LLMs or generated talk tracks.
- Raw TPS bakeoffs, migration claims, or preview-dependent hero flows.
- Provisioning inside the timed race.
