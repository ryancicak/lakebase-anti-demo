# Contributing

Read this before your first change. Two conventions here have already cost
people hours, and both are described below under [Conventions that bite](#conventions-that-bite).

## What this project is

**Lakebase: The Anti-Demo** is a persona-aware competitive proof engine. It runs
one fair, live PostgreSQL task against Lakebase and one real AWS competitor —
Aurora Serverless v2 or RDS PostgreSQL — and produces a single verified number.

The argument it makes is narrow and deliberate: *the same task, the same data,
the same client, the same transaction, and the same verifier, run against both
platforms, with one honest result*. It is called the Anti-Demo because it is the
opposite of a curated dashboard. Nothing is simulated, nothing is pre-warmed for
one side, and a lane that fails stays visibly failed.

That framing constrains contributions more than it might look:

- **No simulation in production code.** Deterministic fakes exist only inside
  automated tests. A UI preview state never displays an invented measurement.
- **A claim needs a receipt.** Every round names what it measured, and equally
  names what it did *not* measure. Rounds 4 and 6 deliberately claim no speed
  margin against AWS, because the competing stack was never built or timed.
- **Fairness is mechanical, not rhetorical.** One monotonic start barrier,
  recorded launch skew, identical TLS posture, fresh connections, same region.

`README.md` is the operator guide, and its
[What has been proven and what has not](README.md#what-has-been-proven-and-what-has-not)
table is the build status of each round. `ROUNDS.md` is the specification — see
[Contracts you must not break](#contracts-you-must-not-break).

## Licensing

**This project is MIT licensed.** The terms are in [`LICENSE`](LICENSE) at the
repository root. Contributions are accepted inbound under those same terms: by
opening a pull request you are offering your change under the MIT licence.

## Rounds cost real money

**Running a round provisions real, billable cloud infrastructure in a real AWS
account and a real Databricks workspace.** This is not a sandbox simulation and
there is no dry-run mode for the rounds themselves.

What a checkout costs you for merely existing is the AWS half — about
**$8.36/day**, from `--apply` until `cleanup`. Deploy the App as well and it is
about **$19.29/day**, because a running App bills for provisioned capacity until
someone stops it. The Round 4 pipeline is **not** a daily line: it is up for the
minutes a bout needs and released afterwards.

Left up for a full day, that pipeline is the largest line here. A full
installation in that state costs **about $22.93/day with the Round 4 pipeline running,
and about $8.36/day with that pipeline stopped**. Add the Databricks App's own compute, about
$10.93/day, and the all-in figure is about $33.86/day, or about $19.29/day with
the pipeline stopped; that lane is excluded from the first figure because it
bills whether or not this project exists, so which pair is yours depends on
whether the workspace was already running an App. `$22.93` and `$19.29` are two
different quantities `$3.64` apart: the subtotal with the pipeline
**running** against the all-in with it **stopped**. Every standing figure here traces to
one sealed receipt — the standing-cost disclosure in receipt `EECDD4D6`, as of
`2026-08-25T02:10Z` — and was priced throughout in `us-west-2`. The pipeline rate
is the one exception and no longer reconciles to that receipt: the receipt
divided that line's posted DBU by the span between its first and last posted
interval, idle hours included, which blends a 62.5% duty cycle into what it calls
a rate. Dividing the same meter by uptime gives $0.61/hour, and that is what is
published here.

The split matters more than either figure. Roughly $8.36 is AWS and roughly
$14.57 is the Round 4 reverse-ETL pipeline. The Databricks lines are posted usage
times a posted price and the disclosure checks itself against its own projection,
coming in 1.7% above it over the shared window. The AWS line is rate-card
arithmetic that **no invoice has ever confirmed** — `ce:GetCostAndUsage` is
denied to this installation, so there is no posted counterpart at all. **That is
where the error bar runs upward**, and it is worth being explicit that this
changed: earlier revisions of this file said the Databricks side was the
unevidenced half, and the disclosure now prices all of it. The `m6i.large` Round 5
runner alone is $2.48/day. Half these figures are a live meter that moves between
seals, so read them as one dated observation on one installation.
[README.md](README.md) carries the itemised figures; read them before you
provision anything.

Round 4 starts the pipeline at arm and schedules its stop 20 minutes after the
bout settles, leaving a redo window. A graceful server shutdown stops it sooner
when that process started it. A stop that fires costs cents: one bout costs about
`$0.32` end to end, and a longer 32.85-minute warm window came to `$0.50`. Read a
window before its posted usage has settled and it reads low, sometimes by a
factor of several, so let a window complete before quoting it. The
pipeline bills for as long as it is up, though, and it does not stop itself if
the server process dies. Check it after a session and stop it if it is still
running:

```bash
./antidemo pipeline status
./antidemo pipeline stop
```

Clone it Friday and forget it until Monday: three days of a local install with
the pipeline released is about **$25**, or about **$58** with the App deployed
too. Databricks Apps have no
idle timeout and do not scale to zero, so `databricks apps stop` is a separate
act from `./antidemo cleanup --yes`.

A single provisioning run creates, at minimum, one Lakebase project, one Aurora
Serverless v2 cluster with a writer, one RDS PostgreSQL instance, and the Round 5
baseline including an `m6i.large` EC2 runner, IAM roles, and Secrets Manager
entries. Individual rounds add more: Round 3 restores full point-in-time recovery
clusters, and Round 5 creates a per-bout RDS Proxy.

Consequences worth internalising before you run anything:

- **Resources bill until they are destroyed.** The `expires-at` tag is an
  ownership signal, not an automatic deletion service. Nothing reaps it for you,
  and since it no longer gates the app either, an abandoned installation will not
  announce itself — it will just keep billing quietly. `antidemo cleanup --yes` is the
  only thing that stops it.
- **In the sandbox this was built in, something else deletes the databases on a
  schedule — and in your account nothing will.** That account is swept by its own
  automation roughly every 14 days, on a Sunday around 02:00 UTC, deleting RDS and
  Aurora with `skipFinalSnapshot: true` and ignoring `expires-at` completely. Much
  of the recovery code here exists because of it. A stranger's own AWS account has
  no such sweep, so read the previous bullet carefully: nothing is going to stop
  the bill on your behalf. If you do run somewhere that sweeps, expect no final
  snapshot, keep nothing you care about only in a provisioned database, and take
  any exemption up with whoever owns the sweep rather than self-applying an
  exclusion tag.
- **Every database it creates has a public endpoint.** All four Aurora writers and
  all three RDS instances are provisioned `publicly_accessible = true`, so each
  has a public DNS name and address; the only thing in front of them is a security
  group admitting your `/32`, the Databricks-published serverless egress prefixes,
  and the Round 5 runner on TCP 5432. That is deliberate — the demo is driven from
  a laptop and from serverless compute, neither of which is in the VPC — but it
  means the blast radius of widening that one rule is direct internet exposure,
  and it may be against your organisation's policy. `README.md` § "Install" and
  [docs/BOOTSTRAP.md](docs/BOOTSTRAP.md#the-databases-are-reachable-from-the-internet)
  have the rules and the reasoning. Keep nothing in these databases you would mind
  losing or exposing.
- **A passed `expires-at` is not a fault.** The app serves, the rounds run,
  `setup` repairs, `cleanup` destroys, and `doctor` reports it as a `WARN` line.
  Use `antidemo renew --ttl-hours N` to move it forward on an existing installation;
  `antidemo setup --ttl-hours` applies only to a first provision and is refused
  elsewhere with a pointer to `renew`.
- **Always finish with cleanup.** Inspect first, then destroy:

  ```bash
  ./antidemo cleanup --dry-run
  ./antidemo cleanup --yes
  ```

- **Cleanup is ownership-scoped**, so it only removes manifest-owned resources.
  That is a safety property, and it also means a resource created outside the
  manifest is *your* problem to find.
- **You can usually avoid provisioning entirely.** Both test suites run against
  deterministic fakes with no cloud access and no credentials. Most contributions
  — UI, copy, verifier logic, adapters — need no live environment at all. Reach
  for one only when you are changing behaviour that only a live round can prove.

If you are picking this up from someone else, assume an environment may already
exist and be costing money. Check before you provision another.

## Getting a local environment running

### Prerequisites

For contributing to code, tests and UI — none of which touch the cloud:

- Python 3.12 or newer (`requires-python = ">=3.12"`), with [`uv`](https://docs.astral.sh/uv/)
- Node.js with `npm`

To run live rounds you additionally need every binary `bootstrap.sh` and
`antidemo doctor` check for. `bootstrap.sh` refuses to start if any is missing:

- `terraform` (>= 1.9.0; >= 1.11 only for the opt-in S3 state backend)
- The AWS CLI and the Databricks CLI
- `psql`
- `jq`
- `python3` on `PATH` (separate from the `.venv` interpreter `uv` manages)

`README.md` has the account-side prerequisites — a Lakebase-enabled workspace, a
service principal with OAuth M2M credentials, a SQL warehouse, a Unity Catalog
for Round 4, and an AWS principal carrying the policies in `docs/iam/`.

### Install and build

```bash
uv sync                     # Python dependencies into .venv
cd frontend && npm ci       # frontend dependencies
npm run build               # writes frontend/dist
```

`npm run build` is not optional if you plan to open the UI. `frontend/dist` is
**not** committed (see [Why `frontend/dist` is not in git](#why-frontenddist-is-not-in-git)).
Without it the server answers every page with a 503 reading *"The application
build is unavailable."* That page is the expected symptom of a missing build, not
a bug.

For UI work, prefer the Vite dev server over rebuilding each time:

```bash
cd frontend && npm run dev
```

### Two files this tree must not contain

`uv sync` is the only command that installs anything, and `pyproject.toml` plus
`uv.lock` are the only place dependencies or interpreter constraints are
declared. Two files that look like they belong here are banned, and each is
refused by `bootstrap.sh`, listed in `.gitignore`, and asserted absent by
`tests/test_deploy_hygiene.py`:

- **`requirements.txt`.** Its presence in the deployed tree makes the Databricks
  Apps runtime install with pip on Python 3.11 whatever `pyproject.toml` asks
  for, and this source does not parse there. Add dependencies to
  `pyproject.toml` and run `uv lock`. If you need a pip-installable export for
  something external, generate it outside the tree:
  `uv export --no-hashes > /tmp/requirements.txt`.
- **`.python-version`.** uv reads it and discards a `.venv` built on any other
  interpreter, so a pin that disagrees with your environment turns the next
  `./antidemo serve` into a silent multi-minute rebuild. `requires-python` in
  `pyproject.toml` is where the constraint belongs; it constrains without
  pinning. To use a specific interpreter deliberately, name it per-command
  (`uv sync --python 3.12`) rather than persisting it in a file.

`tests/test_deploy_hygiene.py` also parses every `.py` file against the oldest
interpreter `requires-python` admits, so syntax that only works on a newer
Python than the project claims to support fails locally rather than in a
deployed container.

### Configuration

If you are standing up a fresh installation, use `./bootstrap.sh` rather than
assembling the environment by hand. It takes five inputs — workspace URL, service
principal OAuth client ID and secret, and an AWS key pair — validates all of them
without provisioning anything, derives everything else including the AWS region
(from `aws configure get region`, so it is only an input where the CLI has no
configured region), `ANTI_DEMO_MANIFEST` and `ROUND5_APP_PRINCIPAL_ARN`, and only
then offers to provision. Its default mode writes exactly one credential-bearing
file — a `~/.databrickscfg` profile, because every Databricks check after it runs
through that profile. It is not otherwise inert on your filesystem: a run that
gets past the preflight gate also creates the generation directory and opens a
`mutation.lock` inside it, so "provisions nothing" is a claim about your cloud
bill and not about your disk. It does not write `bootstrap.json`, and
`--print-env` writes none of the three. See
[docs/BOOTSTRAP.md](docs/BOOTSTRAP.md).

```bash
cp docs/bootstrap.env.example .env.bootstrap   # fill in, never commit
./bootstrap.sh                                 # validates; provisions nothing
```

`./bootstrap.sh` with no flags is also how you check whether a **deployed**
Databricks App is still serving a current seal, and you should assume it is not.
`app.yaml` binds `ANTI_DEMO_MANIFEST_JSON` to the contents of a secret rather
than to a path, so every `antidemo setup`, resume and `antidemo renew` rewrites the
manifest while the deployed app keeps serving whatever was last pushed. Check
mode compares the manifest's hash against a local record of what was pushed and
reports `drift` when they disagree; `./bootstrap.sh --deploy-only` republishes,
redeploys, restarts and then verifies the container actually started. It never
touches AWS and never runs Terraform. A *matching* seal cannot be proven — the
secret value is unreadable through the public API — so it fails toward
"republish". [docs/DEPLOY.md](docs/DEPLOY.md) has the detail, including the parts
that have never been run against a live workspace.

`docs/bootstrap.env.example` also lists `ANTI_DEMO_TF_BACKEND`,
`ANTI_DEMO_TF_STATE_BUCKET` and `ANTI_DEMO_TF_STATE_KEY`, which select an opt-in
S3 state backend. The `_terraform_init` patch they needed has landed, so
`bootstrap.sh` accepts `--state-backend s3` — but no `terraform init` has ever
been run against the generated override, and the path is refused on any
generation that already exists. Terraform state beside the manifest is the
default and is what every existing installation uses. See
[docs/DEPLOY.md](docs/DEPLOY.md#terraform-state-in-s3).

To work against an installation that already exists, or to drive `./antidemo`
directly, `.env.example` documents every variable. **Never commit a real
`.env`** — `.gitignore` excludes it, and `.env.example` is the only version that
belongs in git.

```bash
cp .env.example .env
```

Note that nothing in this repository reads `.env`; it is a reference and a
scratchpad you source yourself. The variable that will stop you immediately is
`ANTI_DEMO_MANIFEST`; see below.

### Running it

```bash
eval "$(./bootstrap.sh --print-env)"   # export the derived environment, or set it yourself
./antidemo setup     # provisions or resumes, then serves — this creates cloud resources
./antidemo serve     # serve only, in the foreground
./antidemo serve --background   # serve detached; survives closing this terminal
./antidemo status    # read-only: which process is really serving this port
./antidemo doctor    # verifies tools, identities, ownership, scale-zero state — not read-only
```

`--background` is the supported way to run a server for longer than a terminal
session. It double-forks through `setsid` so the server ends up in a session of
its own; `nohup` does not do this, because it only ignores `SIGHUP` and the
server still dies with the caller's process group. It also gives the server a
log — `server-<port>.log` beside the manifest — which a supervisor process rolls
at 8 MiB, keeping five. There is no launchd or systemd unit in this tree, and
that is deliberate: see `server/server_launch.py` for why.

Nothing here installs anything as a side effect of being run. `./antidemo` passes
`--no-sync` to `uv run` on every subcommand and refuses up front if `.venv` is
missing, because a dependency resolve triggered by `./antidemo serve` stalls the
demo it is about to start. `uv sync` is the one command that provisions, and it
is always explicit.

`./antidemo doctor` is the command for inspecting a local environment you did not
create, but it is **not** read-only and this document used to say it was. It runs
inside the same generation-wide mutation lock as a provision, because it writes:
it restores the sealed Round 4 baseline in the operator's own Unity Catalog when a
completed run has left its proof row behind (`_restore_round4_baseline_if_owned`,
reached from `_round4_check`), and it wakes Lakebase out of scale zero to check
it, which is why `_lakebase_scale_zero_check` has to remain the last check in the
list. So it is not the thing to reach for beside a bout in progress. What it does
*not* do, despite the comment at `server/cli.py`, is run Terraform or reap
anything: it checks that the `terraform` binary is on `PATH` and it *reports*
orphans as findings rather than deleting them. `./bootstrap.sh --print-env` is
genuinely read-only: it validates and prints, and never provisions.

Be deliberate about `./antidemo setup` on an installation that already exists. It
runs `terraform plan` and `terraform apply` through `reconcile_infrastructure`,
so a pending diff in `infra/aws/` is applied as a side effect of what looks like
a start-up command.

## Conventions that bite

These three are not style preferences. The first two have already caused real
debugging sessions, and none of them fails in a way that looks like a failure.

### `npx tsc --noEmit` is a no-op — use `npm run typecheck`

The root `frontend/tsconfig.json` is a **solution-style** config: it sets
`"files": []` and only lists project references. Bare `tsc` therefore typechecks
**nothing at all** and exits 0.

It does not warn you. It reports a clean tree while the tree is broken. This
silently hid two real type errors.

```bash
# WRONG — always passes, checks nothing
npx tsc --noEmit

# RIGHT
cd frontend && npm run typecheck     # checks tsconfig.app.json AND tsconfig.test.json
```

If you ever see a suspiciously instant clean typecheck, you ran the wrong one.

### `ANTI_DEMO_MANIFEST` is required and has no default

The manifest is the sealed, secret-free ownership record for one provisioned
generation of cloud resources. Every command must be told which generation it
means to act on.

**There is deliberately no default path.** There used to be one, pointing at
`.anti-demo/manifest.json`. It outlived the generation that wrote it, so a bare
`./antidemo` command silently operated on a dead environment while a live one ran
beside it — an entire environment sitting invisible and billing. Refusing to
guess is what makes a second generation visible instead of invisible.

Point it at the **current** generation. The simplest way is not to type the
number at all — `./bootstrap.sh` resolves it and prints the export:

```bash
eval "$(./bootstrap.sh --print-env)"
```

By hand, it is the `.anti-demo-v<n>/` directory with the highest `<n>` on disk:

```bash
export ANTI_DEMO_MANIFEST="$PWD/.anti-demo-v<n>/manifest.json"
```

No generation number is written down here on purpose: `<n>` increments on every
re-provision, so a literal one is wrong for anyone whose install has moved on.
Compare the numbers rather than sorting the names — `.anti-demo-v10` sorts
*before* `.anti-demo-v7`, which is a trap `bootstrap.sh` itself has to work
around. `.anti-demo/`, with no number at all, is an older dead one that may still
be sitting on disk.

If you get *"No owned demo manifest is selected"*,
this variable is unset — set it rather than reintroducing a default. Nothing in
`./antidemo` sets it for you, and that is deliberate rather than an omission: the
launcher *does* read `.env.bootstrap`, but it carries only the five AWS
credential and region variables out of it, precisely so that a stale file cannot
silently point a serve at another generation. So `ANTI_DEMO_MANIFEST` has to be
exported in the shell you run from.

The `--print-env` route above is a convenience at the entry point, not a default
inside the library — `manifest_path()` still raises, deliberately.

Deployed Databricks Apps use a different variable, `ANTI_DEMO_MANIFEST_JSON`,
injected from the app resource `anti-demo-manifest-json`. Do not confuse the two.

### `git clean -xfd` is harmless before you install and destructive after

Reaching for `git clean -xfd` to get a pristine tree is routine, and on a **fresh
clone it is exactly that**: it removes `.venv`, `frontend/dist`,
`frontend/node_modules` and the various caches, and `./bootstrap.sh` rebuilds the
first two on its next run. Nothing is lost.

**On an installed tree the same command destroys your installation's only record of
what it provisioned.** Everything below is gitignored, so `git clean` treats it as
disposable build output:

- `.anti-demo/` and every `.anti-demo-v<n>/` — the generation directories.
  Each holds that generation's `manifest.json` **and its `terraform.tfstate`**, so
  this is the manifest and the local Terraform state in one stroke, along with
  every sealed receipt on disk.
- `.env.bootstrap` — the five credential and region values, which nothing else
  stores.
- `infra/aws/.terraform/` and any `infra/aws/*.tfstate*`.

Cleanup is ownership-scoped and reads the manifest, so without one `./antidemo
cleanup` has nothing to act on. **The AWS resources keep billing with only their
tags left to identify them** — the teardown box at the top of
[README.md](README.md) says so directly, and
[docs/BOOTSTRAP.md](docs/BOOTSTRAP.md#stopping-the-spend) is what you would be
reduced to. `git clean` exits 0 and says none of it.

So check before you run it, rather than guessing:

```bash
git clean -xfdn     # dry run — lists what WOULD be removed, changes nothing
```

If that list names a `.anti-demo*` directory or `.env.bootstrap`, do not run the
real thing.

```bash
git clean -xfd -e '.anti-demo*' -e '.env.bootstrap*'   # keeps both
```

Or clean only the paths you actually meant.

## Tests and checks

Run all of these before opening a change. None of them touch the cloud — the
suites use deterministic fakes throughout, including the files named `*_live.py`,
which test the live *adapters* against fakes rather than live infrastructure.

```bash
# Python: unit and contract tests
uv run pytest

# Python: the one slow test, deselected from the run above
uv run pytest -m slow

# Python: lint
uv run ruff check .

# Frontend (from frontend/)
npm test              # vitest run
npm run typecheck     # NOT `npx tsc --noEmit`
npm run lint          # eslint
```

### Current baselines

| Check | Command | What a healthy answer looks like |
| --- | --- | --- |
| Python tests | `uv run pytest` | All pass; the run prints its own collected count and one deselection |
| The slow test | `uv run pytest -m slow` | Exactly 1 test selected; CI runs it as its own `bootstrap` job — see [The `slow` marker](#the-slow-marker) |
| Ruff | `uv run ruff check .` | `All checks passed!` |
| Frontend tests | `npm test` | All pass; the run prints its own count and file count |
| Typecheck | `npm run typecheck` | Clean, no output |
| ESLint | `npm run lint` | **Failing on `main` as of 2026-08-24 21:07 CDT** — 1 error and 9 warnings, being cleared by separate work. Check `main` before assuming your change caused it |
| Copy guard | not runnable from a clone | See [The copy guard](#the-copy-guard) |

**Exact test counts used to be recorded here and are not any more, deliberately.**
They drifted three times in a single evening — the Python figure was written as
1193 while the suite collected 1750 — and a wrong floor is worse than no floor,
because it invites a contributor to go hunting for tests they never broke. The
commands above print the current numbers, and those are the only ones worth
comparing against. The rule they were standing in for is the durable part: **tests
should go up, and none of the clean checks should start failing.** Compare a run on
your branch against a run on `main`, not against a number in a document.

### The `slow` marker

`uv run pytest` deselects one test:
`tests/test_bootstrap_stub_harness.py::test_bootstrap_shell_paths_hold_against_stubs`,
which drives `bootstrap.sh` against stubbed `aws` / `databricks` / `terraform`
binaries. It was 310 of the suite's 344 seconds — 90% of the wall clock for one
test — and without it the suite answers in about 34. Every run prints the
deselection, so nothing is hidden.

It is not optional coverage: it is the only thing exercising `bootstrap.sh`'s
refusal branches, which are the ones that spend money, and `bootstrap.sh` is the
first thing a stranger executes. Run it with `uv run pytest -m slow` before any
change to `bootstrap.sh` or `tests/bootstrap_stub_harness.sh`; CI runs it in its
own `bootstrap` job on every push.

`-m slow` is required even when you name the test by node id — the default
`-m 'not slow'` wins otherwise, and the run reports no tests rather than an
error.

### The copy guard

**This is not a check you can run, and nothing in this repository enforces it.**
The copy contract lives in `ROUNDS.md` and is prose rather than a build gate:
nothing here scans for the banned variants and no CI job reads copy. A
`check_copy.py` scan does exist — a deterministic scan for banned or drifted
user-facing phrasing — but it sits in a sibling scratch directory on the author's
machine, outside this repository, so a reader who clones this tree cannot run it.
Treat it as unavailable and read the contract table in `ROUNDS.md` by hand instead.
That is why the section above lists it as not runnable rather than as something to
run before opening a change.

The guidance itself still holds, and it is the part that transfers. Match the
contract table's phrasing; each rule there states why it exists. If you genuinely
need an exception, end the offending line with `copy-audit: allow <rule-id>` — the
marker that scan honours, if it is ever wired in here — **and** update the contract
table in `ROUNDS.md`, which is the part a reader actually has. An exemption without
a contract update is a copy change made by the back door.

For the record, since the numbers were previously quoted here as a baseline: the
last recorded scan reported five violations, three of them in this repository — two
`towel-verdict-is-lane-status` in `frontend/src/App.test.tsx` and one
`preview-means-non-executable` in `server/catalog.py` — with the other two being the
same `App.test.tsx` strings inside a timestamped backup in the scratch tree, which
nothing in this repository can clear. That is a dated observation from a tool you
cannot run, not a threshold to compare against.

### The credits roll's stylesheets

`frontend/src/credits.css` and `frontend/src/credits-entry.css` are both ordinary
hand-authored sheets — edit either one directly. They split the roll between
them: `credits.css` dresses the scene and everything that scrolls through it, and
`credits-entry.css` dresses the authorship card, the held mark after the crawl,
and the footer control that opens the roll. Separately, a standalone
double-clickable Desktop copy of the credits is built by
`../lakebase-anti-demo-scratch/credits/desktop-build/build.py`, which lives in the
same sibling scratch tree as the copy guard and carries its own private copies of
both stylesheets. `credits.css` used to be generated from one of them; it is not
any more. Nothing links the two now, so changing the app's credits styling does
**not** update that Desktop artifact, and it is not built by anything in this
repository.

## Contracts you must not break

`ROUNDS.md` is not background reading. It holds two contracts that changes are
judged against:

1. **The acceptance contract** — the specification of what each round measures,
   what fairness means mechanically, what the definition of done is, and what is
   explicitly out of scope. If a change alters what a round measures or claims, it
   changes this contract, and the document must change with it in the same
   review.

2. **The copy contract** — one canonical phrase per concept, in a table, with the
   exact places each phrase may appear. `VERIFIED` is a technical lane state, not
   a slogan. A round outcome, a lane status, and a banner promise are three
   different things and may never be substituted for one another. Five variants
   are banned outright, each because it asserted something its own artifact could
   not support.

Read both before changing user-facing strings. The copy guard enforces only the
banned variants; the rest of the contract is enforced by review.

## Conventions

- **Style is enforced, not debated.** Ruff (line length 100, target py312) and
  ESLint decide. Run them rather than matching by eye.
- **Match the surrounding code.** Comment density and naming included.
- **Comments explain why, not what.** The codebase uses them to record
  constraints and hard-won history — see the `ANTI_DEMO_MANIFEST` comment in
  `server/manifest.py` for the intended shape. Do not narrate the code.
- **Never commit secrets.** No credential belongs in source, tests, snapshots,
  logs, Terraform state, or browser payloads. See below for what is already
  excluded.
- **Keep live state out of git.** Manifests, Terraform state and plans, receipts,
  server logs, and PID files are per-environment artifacts. They are ignored, and
  they carry the AWS account ID, resource identifiers, endpoint hostnames, and
  operator identity.
- **No hand-made `.bak` files.** Before this repository had history, contributors
  saved timestamped copies like `App.tsx.bak.20260820-172941` as a safety net.
  Git is that safety net now. `*.bak` and `.backups/` are ignored; commit a
  work-in-progress branch instead.

### Why `frontend/dist` is not in git

It is build output. Committing a fingerprinted bundle produces unreviewable
diffs, constant merge conflicts, and — as this project has already experienced —
a stale bundle that silently disagrees with the source it was built from.

Excluding it is safe for deployment: a Databricks App is deployed by syncing the
working tree, not by cloning this repository, so the build simply has to exist
locally at deploy time. Run `npm run build` before serving or deploying. A
missing bundle produces the explicit 503 page described earlier rather than a
silent failure.

### A note on the secret scanner

If you work at Databricks, the managed pre-commit hook has a
`linkedin-client-secret` rule that fires on `frontend/src/App.tsx`. **It is a
false positive.** The rule matches the literal text `LinkedIn post` followed by a
colon and any 16-character token; the token it finds is the React state variable
`cardRenderFailed`, which is exactly 16 characters long. There is no credential
in that file.

That line now carries an inline `{/* gitleaks:allow … */}` marker, which is what
silences it, so an ordinary commit is not blocked. Two things follow from how the
scanner works, and both are easy to get wrong:

- **Do not "fix" it by renaming the variable.** The name is not the defect, and
  the next 16-character identifier put there would fire again.
- **Keep the marker on the same line as the string.** Verified by running it: a
  marker on the preceding line does not suppress the finding.

The reason this looks intermittent is that the scanner only reads **added**
lines. Once that line is committed and unchanged, no later commit re-scans it, so
deleting the marker appears to cost nothing — right up to the next commit that
touches the line, or the first commit of a fresh repository, where every line is
an addition and it fires again.
