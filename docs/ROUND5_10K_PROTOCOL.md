# Round 5: bell-to-10,000 protocol

Protocol: `round5-bell-to-10k-v4`

Normative implementation contract: `docs/ROUND5_BELL_TO_10K_DESIGN.md`.

This document is the concise operator-facing protocol. The v4 bell-to-10k design in
`docs/ROUND5_BELL_TO_10K_DESIGN.md` is the normative, authoritative contract wherever additional
detail is required. (The `_v3` suffix on the runtime wire names below — `run_lane_v3`,
`round5-resident-control-v3` — is the resident control wire-contract version and is current under
this v4 protocol; it does not refer to an older design.)

## Lifecycle

Round 5 is prepared automatically:

```text
WARMING -> READY -> CLAIMED -> RUNNING -> CLEANING -> WARMING
```

Startup and exact cleanup both wake the durable installation-scoped warm coordinator. Session
creation, round selection, `/arm`, and `/run` never perform slow warm work. A stale or missing warm
generation disables Round 5 while the coordinator repairs it backstage.

`/arm` is an idempotent, O(1) compatibility claim against an already-`READY` generation. It performs
coordination-store compare-and-swap only: no AWS, Databricks, SSM, hostname discovery, capacity
measurement, credential minting, or topology scan.

A warm generation proves:

- distinct Lakebase and competitor `c7i.2xlarge` physical runners, their boot identities, installed
  images, and one-lane capacity receipts;
- the Lakebase pooled/direct bindings and credentials;
- both Aurora and provisioned-RDS source, role, secret reference, TLS, auth, VPC, subnet, and static
  Proxy-network fixtures;
- exact absence of every per-bout RDS Proxy and mutable Round 5 journal debt;
- fresh rotating control-plane and per-runner dispatch capsules.

Warming never creates the per-bout Proxy.

## Bell and clocks

The accepted durable bell transition creates one `bell_id`, one UTC display timestamp, and one
server monotonic origin. Both lane gates are released in the same event-loop turn. `/run` returns a
`RUNNING` projection immediately and awaits no provider or lane work.

Both large UI clocks always mean:

```text
accepted bell -> server observation of exactly 10,000 authenticated, simultaneously held clients
```

They advance from the first `RUNNING` response, before provider progress exists. A terminal lane
stops at `bell_to_10000_observed_ms`; its sibling continues from the same bell. Browser interpolation
uses `performance.now()` seeded from the server floor, accepts only non-regressing revisions, and
never changes the clock to setup time or runner-local ramp time.

## Lakebase lane

The first post-bell external operation is the deterministic `run_lane_v3` dispatch to the dedicated
Lakebase runner. Its first retained client:

1. connects through the included pooled endpoint;
2. authenticates and executes the readiness transaction; and
3. remains client 1 of the 10,000-client set.

Clients 2–10,000 then fan in immediately. There is no post-bell host discovery, STS call, capacity
preflight, or standalone verification transaction.

## AWS lane

The first timed AWS mutation is `CreateDBProxy` with the deterministic bout identity and the sealed
static network fixture. The selected Aurora/RDS lane dispatches `run_lane_v3` immediately after all
of these exact gates pass:

- expected Proxy identity and endpoint are `available`;
- the default target group has the sealed pool settings;
- exactly the selected source is registered and `available`;
- engine family, role, secret/auth, TLS, VPC, subnets, Proxy group, database group, and runner group
  match the claimed warm receipt; and
- journal ownership and the bout fence are current.

The first competitor client performs the real transaction through that Proxy and remains in the
10,000-client set. It never waits for Lakebase.

## Physical runners and idempotency

Each lane has a distinct physical runner, adapter, lock, durable job registry, progress stream,
active identity, task, cancellation owner, and settlement deadline. Capacity projects one 10,000
client lane per runner.

The logical job identity is:

```text
sha256(protocol + warm_generation + bell_id + lane_id)
```

The runner atomically owns or rejoins the job before opening any socket. Repeating an ambiguous SSM
send with the same job ID returns or waits for the existing logical job; a different prepared-request
digest is refused. Result and state remain queryable from the persistent registry after the first SSM
invocation exits.

## Exact proof gates

A lane verifies only when all of these hold:

- exactly 10,000 initiated, authenticated, distinct, and simultaneously retained clients;
- zero terminal failures, retries, and hold disconnects;
- at least a 30-second hold;
- 64/64 sparse queries;
- multiplexing, identity, observer separation, clean-start, fairness, versioned hard-safety
  evidence, and cleanup gates; and
- no measured memory-reserve, file-descriptor-reserve, or ephemeral-port-reserve exhaustion.

Each of the four worker shards must first prove exactly 2,500 initiated, authenticated, and
currently held clients. After all four proofs arrive, every shard re-reads its live sockets and
publishes a fresh `HOLD_PREPARED` proof. Only those four fresh proofs can commit the one shared hold
epoch. A disconnect, hard-safety failure, connection/protocol failure, cancellation, worker crash,
or the one absolute run deadline aborts before the hold and sampling barrier.

Event-loop lag, host scheduling delay, CPU utilization, calibration time, and selector/fanout
timing are advisory pacing signals. They may reduce future admission concurrency and remain visible
in the evidence, but they never cancel admitted connections, block the hold, shorten the 30-second
hold, suppress the 64 samples, or invalidate an otherwise exact result. The 50 ms event-loop and
0.85 CPU thresholds remain adaptation thresholds, not pass/fail gates. Unknown safety codes fail
closed as protocol errors; they are never classified by string prefix.

9,999 fails. A one-sided exact result may remain visible but never declares a winner.

## Failure and cleanup

A fatal lane failure fences new dispatch, stops the sibling and outstanding Proxy work, and enters
cleanup. Every terminal path—verified, failed, toweled, lease loss, or restart—settles or records
both logical runner jobs, deregisters/reset targets, requests Proxy deletion, tears down mutable
resources in reverse dependency order, and proves exact absence.

Terminal publication is absorbing for one bell. It closes progress ingestion before cleanup starts,
allocates the next canonical revision under the session lock, and preserves the terminal verdict
while cleanup state changes. A late progress callback cannot resurrect `RUNNING`; a towel request
that loses the race to a terminal result returns that authoritative result idempotently.

Only that exact proof commits `CLEANING -> WARMING(N+1)`. It closes the old claim, increments the
generation, clears cleanup debt, and wakes the coordinator automatically. No operator action starts
the next warm.
