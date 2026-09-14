# Round 5: exact 10,000-client fan-in protocol

Status: implemented locally; **no exact dual-10,000 live acceptance exists**  
Protocol version: `round5-fanin-v2`  
Stable round ID: `survive_connection_spike`

Every measurement quoted below is historical evidence from an earlier live
attempt. None of it proves the current tree was deployed, and the highest
achieved counts (9,915 Lakebase / 9,919 Aurora) were generator-quality stops,
not database limits.

## Claim boundary

Round 5 tests authenticated PostgreSQL **client connections to pooled
endpoints**. Lakebase's 10,000 figure is PgBouncer `max_client_conn`; it is not
10,000 PostgreSQL backends and it is not 10,000 simultaneous transactions. The
AWS lane is the selected managed RDS Proxy reference path for either Aurora
Serverless v2 or RDS PostgreSQL. The result does not claim that AWS universally
requires RDS Proxy: direct connections, PgBouncer, and application pools remain
valid untested alternatives.

The protocol separates four quantities:

1. client fan-in: authenticated sockets concurrently held at the pooled endpoint;
2. connection churn: socket/TLS/provider-selected native-password setup during
   the ramp;
3. backend multiplexing: direct-observer backend sessions serving those clients;
4. transaction capacity: explicitly not tested, beyond sparse `SELECT 1` samples.

## Phase 0: generator preflight

Preflight runs on the sealed `m6i.xlarge` without opening the 20,000 test sockets.
Fresh setup derives the minimum from these frozen dual-lane constants and
refuses `m6i.large` before Terraform provisions anything. The xlarge shape is
the automatic five-input default; the client count, 768 MiB reserve, FD reserve,
hold, and telemetry gates are not reduced to fit a smaller runner.
It records the actual CPU count, physical/available memory, process RSS, FD
soft/hard limits and current use, ephemeral-port range and current socket-state
counts, network counters, and bounded event-loop/CPU calibrations, including the
exact mirrored micro-batch scheduler quantum.

The capacity model is deliberately conservative:

- known one-lane calibration: 10,000 held TLS/native-password clients used
  3.11 GiB RSS and 10,007 FDs on this runner family;
- projected two-lane held RSS is twice that observed value plus measured
  process baseline;
- projected ramp peak adds 15% TLS/authentication working headroom;
- at least 768 MiB of physical memory remains outside the runner;
- projected FDs plus 256 control FDs stay below 80% of the measured soft limit;
- each lane needs 10,000 ephemeral ports to one remote tuple, with at least
  2,000 ports of lane-local reserve;
- four CPUs (one per pinned worker), the sealed instance type, loop calibration,
  selector-amplification, and baseline pressure guards must all pass.
- during ramp and hold, available memory never drops below 768 MiB, open FDs
  stay below 80% of the soft limit, event-loop p99 stays at or below 50 ms, and
  average process CPU remains at or below 85% of the runner's total CPU
  capacity.

The preflight emits a deterministic JSON decision and model digest. A refusal
means **NOT READY**. It does not resize, create infrastructure, or fall back to
sequential lanes.

## Phase 1: pooled-path setup (supporting metric)

The existing database-only declared start remains intact. Both setup workflows
launch from their existing shared setup T0, and each retains its exact stop-gate
evidence and elapsed time. These values are displayed and sealed as a separate
supporting metric. They do not decide the Round 5 winner.

No fan-in work starts until both pooled paths are verified ready and the runner
passes the following direct-path gates:

- exact pooled host and direct host match the seal;
- TLS is `verify-full` with the sealed trust-bundle digest;
- client and observer native roles match the sealed contract;
- client and observer credential-file digests match the request;
- direct observers use the separate observer role and direct endpoint;
- the dedicated client role has zero pre-existing sessions, regardless of
  application name.

Because the Lakebase setup lane can finish before the selected AWS Proxy path,
both direct observers receive the same bounded 120-second readiness window
before T0. Immediate connection refusals are retried symmetrically inside that
window; authentication and role failures still fail without retry. This wakes a
path that suspended while its peer was provisioning before the shared scored
T0; after release, every client-ramp pause remains included in that lane's time
to 10,000.

The per-bout RDS Proxy target group sets `MaxIdleConnectionsPercent` to zero.
After the exact setup transaction, the setup workflow drains and rebinds the
same journaled target, then waits for it to return to `AVAILABLE`. Both direct
observers still allow up to 120 seconds for any connection-state propagation,
then prove the dedicated client role has zero sessions. A session still present
at that boundary fails the contamination gate; it is never subtracted from the
observer count.

## Phase 2: primary 10,000-client fan-in

One standalone Python 3.12 event-driven implementation drives both lanes. It
uses nonblocking sockets and one identical PostgreSQL TLS/native-password state
machine per connection; there is no thread or process per connection and
psycopg is not used for the 20,000 pooled clients.

Both lanes require TLS `verify-full` against the sealed CA and exact endpoint
hostname before any password bytes may be sent. PostgreSQL servers select the
password exchange. The state machine accepts only cleartext-password inside
that already verified TLS channel or SCRAM-SHA-256 with exact server-signature
verification. It rejects cleartext before TLS, MD5, trust/no-challenge,
unsupported SASL, malformed or reordered challenges, and mid-connection method
changes. "Cleartext-password" is PostgreSQL wire terminology; it is not a
plaintext network connection. The password message is written directly to the
verified transport and is never logged or retained as evidence.

The selected method is sealed per lane as provider-selected evidence and its
full cost remains inside connection timing. Fairness requires the same
native-password role model, client implementation, scheduler, timeouts, query,
and measured boundaries; it does not require two providers to select the same
PostgreSQL authentication method. A lane mixing methods across its 10,000
clients fails identity and fairness validation.

SCRAM nonces and proofs remain client-specific. The expensive salted-password
derivation is cached only inside the one runner process, keyed by password
fingerprint, server salt, and iteration count. PostgreSQL supplies one verifier
salt per role, so recomputing PBKDF2 for every connection would block the single
event loop for a whole launch wave even though its result is identical. The
run-local cache removes that generator artifact without sharing a nonce, proof,
socket, timer, or measured result between clients.

Configuration is one immutable object shared by both lanes:

- target: exactly 10,000 clients per lane;
- shared monotonic T0, released only after both lane coordinators report ready;
- four symmetric worker processes, each owning exactly 2,500 clients per lane;
- one shared parent release and equal alternating schedulers in every worker;
- two clients per lane per worker micro-batch, interleaved identically
  inside each equal adaptive wave;
- at most 32 connects in flight per lane per worker (128 per lane across the
  four workers). The micro-batch fixes mirrored interleaving; this bound fixes
  pipeline depth. They are separate: awaiting each micro-batch to finish
  authenticating made the ramp latency-bound at five to six round trips per
  client, so a worker's 2,500 clients per lane became 1,250 serial waits and the
  loop idled between them;
- identical adaptive waves for both lanes;
- no retries (`max_retries = 0`), so any terminal connect/auth failure fails the
  exact gate and its elapsed cost remains visible;
- 30-second minimum hold;
- 64 sampled clients per lane, divided into eight unsynchronized groups of
  eight across the hold;
- identical connect, TLS, auth, query, and overall deadlines.

Adaptive launch waves may reduce or pause the shared launch rate only when a
whole-run pressure guard fires. The same decision applies to both lanes and all
pause/ramp time remains on the scoring clock. It may never tune one lane
independently. A hard pressure guard stops all launching immediately.
The same hard guard remains active throughout the 30-second hold; a breach
ends the hold and invalidates the telemetry, hold, and sample gates rather than
publishing a database result from a distressed generator.

The original five-per-lane micro-batch was grounded in an on-runner `cProfile` of the
failed 100-client first wave: 300 synchronous `_ssl.SSLSocket.do_handshake`
calls consumed 194 ms of 289 ms total runner time. A later exact retry reached
3,655 held clients per lane without a connection failure, then stopped at
59.914 ms loop delay. A no-endpoint loopback reproduction at the same 7,320-FD
scale measured the old synchronous telemetry sampler at 28.355 ms p50 and
34.891 ms p99: FD enumeration cost 13.942 ms p50 and `/proc/net/tcp` parsing
cost 13.446 ms p50. Telemetry was therefore materially loading the loop it was
supposed to observe.

The 50 ms runtime gate remains unchanged for generator-owned blocking.
Resource telemetry uses an optimized FD counter and runs off-loop every 250 ms,
while an independent 10 ms loop heartbeat records raw wall lag during every
wave. Each heartbeat also records process and loop-thread CPU deltas, Linux
run-queue wait and context switches when available, active TLS handshakes,
selector and ready-queue batch sizes, the latest internal phase, and overlapping
GC pause time. A wall delay above 50 ms is a generator failure only when local
CPU, GC, or an internal callback batch corroborates that the generator owned
the stall.
The runner pre-resolves both hosts before shared T0 and records GC pauses by
generation, selector wakeup batches, ready-queue batches, SSL handshake wall
time, protocol/authentication processing, task creation, progress writing,
telemetry sampling, and observer-query time. Micro-batching still bounds
selector/TLS callback work and yields between quanta; every yield, telemetry
sample, and ramp quantum remains inside the shared-T0 score.

The 8,304/8,307-client instrumented retry localized the remaining owned stalls
to aggregate callback bursts, not an individual TLS or authentication call.
The worst worker/CPU 1 envelope recorded 109.544 ms wall lag, 78.310 ms loop
thread CPU, 119.231 ms process CPU, 618 ready callbacks, no selector return in
that heartbeat, one active handshake, no GC pause, and a 137.476 ms telemetry
sample. Other workers recorded 86.425 ms with 977 ready callbacks and 72.866 ms
with 1,440 ready callbacks; individual authentication, protocol dispatch, TLS
post-processing, and task-creation spans all remained below 1.04 ms.

The generator bounds neither ready callback drain nor selector admission, and
must not. Both caps were removed after measurement showed they inverted the
quantity they were meant to protect. Ready callbacks retain strict FIFO order,
no `SelectorKey` object is retained across iterations, and the full ready
backlog remains visible in stall envelopes. Global `/proc/net/tcp` socket
state telemetry is collected only by worker 0 and rate-limited to once per
second; the parent reuses that system-wide sample instead of making four
identical GIL-bound scans every 250 ms. Telemetry remains off-loop, and
per-phase maxima/totals now separately report selector waits, TLS wall and CPU,
SCRAM proof/signature work, protocol parsing, client allocation, socket-state
scans, progress aggregation, result-queue publication, and final JSON/gzip.
The no-endpoint preflight creates a 256-socket local fanout with 150 µs of
representative callback CPU per socket. 256 is the whole-runner in-flight bound,
four times the concurrent readiness any single worker loop can hold, so it is
deliberate headroom. The gated measurement is the production configuration and
its heartbeat peak must remain at or below the unchanged 50 ms generator-owned
limit; the historical capped configuration is measured only as recorded
evidence and is never selected.

Preflight additionally gates **selector wakeup amplification**: total selector
wakeups divided by delivered readiness events, which must stay at or below 1.05.
Under level-triggered epoll a descriptor dropped at the poll boundary stays
ready and is re-reported on the next poll, and `selectors.py` has already paid a
kernel copy-out plus one key lookup and tuple for every descriptor in the batch
before any truncation occurs. Servicing N ready descriptors K at a time
therefore costs about N²/2K wakeups instead of N. Measured on the 256-socket
probe: the production configuration delivers 1.00× with zero deferred events,
while the historical K=16 cap delivers 8.50× with 240 deferred — and the capped
configuration reports a 4.3 ms heartbeat against the production configuration's
38.9 ms while being *slower* end to end.

That is why a per-turn latency gate cannot be the only signal. The sealed 50 ms
limit is unchanged and was not relaxed; the amplification gate exists because
the per-turn gate structurally rewards the configuration with worse total ramp
time, and total ramp time is what the sealed 60-second Lakebase idle window
actually constrains. `peak_deferred_selector_events` had no write site at all
before this, so the one field that could have quantified the amplification
reported zero in every configuration.

The first bounded live confirmation reached 9,915 Lakebase and 9,919 Aurora
authenticated clients, versus roughly 8,300 before event bounding. Its residual
98.426 ms stall was no longer an individual function hotspot: worker/CPU 2,
wave 25 had 3,819 queued callbacks, 106.738 ms loop-thread CPU, 1.892 ms
scheduler wait, no GC, no active handshake, and only three newly delivered
selector events. The heartbeat coroutine itself was queued behind that backlog.
The authoritative monitor remains in the same FIFO as ordinary callbacks so
its delay cannot be masked by append-left priority. Each delayed heartbeat
records per-interval callback CPU with sampled callback or Task-coroutine
names, oldest FIFO-ready age, `_run_once` CPU, loop-thread scheduler wait,
telemetry helper native thread and CPU, and native SSL handshake call/CPU
totals. Full ready backlog remains explicit in every envelope.

That 9,915/9,919 attempt is the measurement that identified the caps as the
cause rather than the cure. Loop-thread CPU of 106.738 ms inside a 98.426 ms
interval means the loop was never blocked or waiting: it was CPU-saturated
servicing callbacks, with arrival outrunning service. Backlog had grown from 618
callbacks before the caps to 3,819 after them, a sixfold loss of queue
stability. The reported "three newly delivered selector events" was an artifact:
that field read `last_selector_batch`, overwritten on every `select()` call, so a
delayed heartbeat spanning roughly sixty polls reported only the final one. The
envelope now reports the interval maximum. Both lanes receive exactly the same
scheduling policy, and all extra turns remain on the shared-T0 ramp clock.

Raw wall lag is never discarded or relabeled: it has separate raw and external
peaks plus warning counts. Three external wall stalls above 250 ms fail with
`host_scheduling_instability`; 250 ms is five times the generator-owned limit
and 25 missed 10 ms heartbeats, so repeated breaches identify a host that is not
stable enough to certify a timed result without turning ordinary network/TLS
wait into generator saturation.

The first instrumented retry attributed a 73.977 ms loop stall to a 73.301 ms
generation-2 GC pause; authentication processing was at most 1.260 ms, TLS
post-processing 2.879 ms, the ready batch 15, and selector wakeups 10. After
controlled GC removed that cause, a five-per-lane TLS quantum still produced a
59.347 ms stall at 250 held clients, with no measured GC pause during ramp and
all instrumented Python callback sections below 1.588 ms. The mirrored quantum
was initially one client per lane per worker. The single-loop confirmation then
reached 2,070 per lane in 62.76 seconds, at which point the Lakebase path's
60-second idle policy had closed 1,767 early sockets and one selector wakeup
batch reached 1,190 events. Four symmetric processes are therefore required to
finish the ramp inside that sealed idle boundary while keeping each loop's TLS
quantum bounded. The parent releases all workers from one monotonic T0,
requires four exact partitions per lane, and starts one shared hold only after
every partition is complete. Both lanes remain concurrent, and every worker's
ramp time remains in each shared-T0 score.

The first process-sharded run proved that unpinned workers were not symmetric:
one worker reached 2,104 clients per lane while the others stopped at 56, 56,
and 28 after OS descheduling produced 53–67 ms loop stalls. Each worker is now
pinned to one distinct xlarge CPU, and the measured process preflight requires
four unique PIDs and four unique CPU affinities. The latest pinned retry showed
raw 53.497–72.929 ms loop-wall peaks while authentication callbacks stayed at
or below 1.229 ms, ready batches stayed at 2, selector batches stayed at 3, and
TLS handshake wall time tracked the raw lag. That iteration reduced the launch
quantum to one client per lane per worker, which is superseded: the mirrored
quantum is two and in-flight connects are bounded at 32 per lane per worker.
Adaptive pacing remains one whole-run symmetric decision, and every added pause
remains scored.

Each client has a deterministic lane/ordinal identity. At the gate the runner
proves 10,000 unique live transport objects, 10,000 worker-namespaced live FDs,
and 10,000 unique local socket endpoints for each lane. The parent refuses
missing, duplicate, underfilled, or overfilled partitions. A digest binds those
identities and worker partitions without emitting all 20,000 records.

Each lane's primary result is `time_to_10000_ms`, measured from the shared T0 to
the first instant all 10,000 distinct sockets are authenticated and
simultaneously held. A winner and raw margin exist only when both exact lane
gates verify under the same protocol/config/model digests. A tie is exact
nanosecond equality. A 9,999 count can never pass.

## Hold and sparse transaction sampling

After both lanes reach 10,000, all 20,000 clients remain open for at least 30
seconds. The hold starts at the later lane gate so both lanes receive the full
common interval.

Eight-client sample groups are offset within each hold window and between
lanes. They issue only `SELECT 1`; no synchronized transaction load is
generated. Every selected connection must return exactly one row and return to
ReadyForQuery. Any disconnect or sample failure invalidates that lane.

Dedicated observer connections use direct endpoints and a separate observer
role, so they consume none of the tested pool's 10,000 client slots. Before T0
they prove zero client-role sessions. During ramp/hold they sample
`pg_stat_activity` for the client role and record current/peak backend sessions
under one connection owner per observer. A dropped or broken observer transport
may reconnect at most twice with the same direct host, verify-full trust,
observer credential, and role check. Retry count and elapsed time remain in
evidence. SQLSTATE identity/auth/query/result failures do not retry; exhaustion
fails the explicit observer verification gate instead of crashing a worker.

Observer evidence records current and peak backend sessions and backend PIDs.
Multiplexing passes only when at least one backend session is
observed and the peak is strictly below 10,000. Backend session counts are
evidence about fan-in and are never reported as client counts.

## Telemetry and progress

The runner emits bounded newline-delimited `PROGRESS_JSON:` records and one
compressed `RESULT_GZIP_BASE64:` record. The schemas carry explicit
`schema_version`, `protocol`, `config_sha256`, `generator_sha256`, and
`capacity_model_sha256`; stale v1 payloads fail closed.

Progress records are monotonic by lane and sequence. They include phase,
authenticated/held/failed counts, elapsed time from T0, hold remaining,
sample-query successes/failures, backend-session observations, and whole-run
CPU, RSS, FD, socket-state, ephemeral-port, network, and event-loop-lag
telemetry. Output is milestone/rate limited so SSM output cannot truncate the
terminal receipt. SSE refresh/reconnect accepts only a nondecreasing server
snapshot.

The terminal evidence retains the number of telemetry samples, peak process CPU
share, peak RSS, minimum available memory, peak FD use, FD limit, event-loop p99
peak, ephemeral-port count and minimum per-lane reserve. Those fields and an
empty pressure-failure list are independently required; a bare
`telemetry_verified: true` is not sufficient.

The terminal lane gate requires all of:

- exactly 10,000 distinct initiated sockets;
- exactly 10,000 authenticated and simultaneously held sockets;
- zero terminal connection/auth failures and zero retries;
- at least 30 seconds of hold with zero disconnects;
- all 64 sparse sampled queries successful;
- direct-observer current/peak backend count and PID evidence proving
  many-to-few multiplexing;
- sealed host, TLS, supported provider-selected authentication method,
  native-role, credential, config, generator, and model digests;
- zero pre-existing client-role sessions;
- observer/controller separation from the tested pool.
- runtime telemetry below every frozen pressure threshold, with no recorded
  pressure failure.

## Towel, failure, and cleanup

A towel or hard guard atomically freezes each lane's exact achieved count,
elapsed lower bound, phase, and any lane that already reached its exact 10,000
gate. It never manufactures a comparison from one exact result and one lower
bound.

Cancellation sets the shared stop flag, stops new launches immediately, cancels
pending connects, closes every authenticated transport deterministically,
settles observer tasks, and verifies:

- zero runner-owned pooled sockets;
- zero dedicated-client-role pooled clients;
- zero fan-in child processes (the protocol normally creates none);
- no run directory and the flock released.

The existing SSM cancellation/settlement and journaled reverse cleanup then
delete the run-owned RDS Proxy, IAM/network changes, and secret material. Any
socket, process, command, role-client, Proxy, journal, or lease debt blocks
share/redo and leaves the cleanup lease held.

## Scoring and publication

Phase 1 setup elapsed values are supporting metrics. Phase 2
`time_to_10000_ms` is primary. Both exact stop gates plus hold, sparse-query,
multiplexing, fairness, identity, telemetry-integrity, and cleanup gates are
required before a winner/margin is published.

If only one lane reaches 10,000, that exact lane result may be preserved beside
the other lane's achieved count and elapsed lower bound, but the bout has no
winner or margin. A generator-pressure stop is a capacity decision, not a
database loss. Receipts, replay, recap, scorecard, cost copy, and all persona
SHOW sections use the same classification.

Historical protocol names may remain only in compatibility/migration code.
No v1 128-attempt or 64-client witness payload can be interpreted as v2 proof.
