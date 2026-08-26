# Bootstrapping from five inputs

`./bootstrap.sh` is the single entry point from credentials to a stage-ready
installation. It takes five values — four secrets and a workspace URL — derives
everything else, provisions the local environment it needs, validates all of it
before spending anything, prints what the spend will be, and then runs
`./antidemo setup`.

> **`--apply` spends real money and the installation does not expire.** What you
> pay for merely having this installed is the AWS half: about **$8.36/day**, from
> `--apply` until `cleanup`, whether or not anyone runs a round. Deploying the App
> as well makes it about **$19.29/day**, because a running App bills for its
> provisioned capacity until someone stops it. Round 4's pipeline is **not** a
> daily cost — it runs for the minutes a bout needs and is then released.
>
> Left up for a full day that pipeline reads higher than anything else here:
> about **$22.93/day with the Round 4 pipeline running, and
> about $8.36/day with that pipeline stopped**. Add the Databricks App's own
> compute — about **$10.93/day**, which bills whether or not this project exists
> — and the all-in figure is about **$33.86/day**, or about **$19.29/day** with
> the pipeline stopped. Which of the two pairs is yours depends on whether that
> workspace was already running an App. `$22.93` and `$19.29` are two different
> quantities `$3.64` apart: the subtotal with the pipeline **running**
> against the all-in with it **stopped**.
>
> Of the $22.93: ~$8.36 is AWS and ~$14.57 is the Round 4 pipeline. The AWS half
> is **rate-card arithmetic that no invoice has ever confirmed** —
> `ce:GetCostAndUsage` is denied to this installation, so there is no posted
> counterpart to check it against, and that is where the error bar now runs
> upward. The Databricks half is posted usage times a posted price, and the
> disclosure checks itself: posted came in 1.7% above its own projection over the
> window the two share.
>
> **Every standing figure here traces to one sealed receipt** — the standing-cost
> disclosure in receipt `EECDD4D6`, as of `2026-08-25T02:10Z` — **and was priced
> in one region, `us-west-2`.** You choose your own, both vendors price per
> region, and half these figures are a live meter that moves between seals. Treat
> all of it as that installation's bill on that night rather than yours. The
> Round 4 pipeline rate is the one exception and no longer reconciles to that
> receipt: the receipt divided that line's posted DBU by the span between its
> first and last posted interval, which included every hour the pipeline was
> stopped, so its `$11.07/day` blends a 62.5% duty cycle into what it calls a
> rate. Dividing the same meter by uptime gives `$0.61/hour` — `$14.57/day` — and
> that is the figure above.
>
> The base $8.36 accrues whether or not anyone runs a round. The pipeline is not
> meant to stay resident after Round 4: arm starts it, settlement schedules a
> stop after a 20-minute redo window, and graceful shutdown stops it sooner when
> that process started it. A stop that fires costs cents: one bout costs about
> `$0.32` end to end, and a longer 32.85-minute warm window came to `$0.50` once
> its posted usage had settled. Posted usage lags by hours, so a window read
> early is a watermark rather than the finished line and reads low. That line bills for as
> long as it is up, and it does not stop itself if the server process dies, so
> check it after a session and stop it if it is still running:
> `./antidemo pipeline status`, then `./antidemo pipeline stop`.
>
> **Clone it Friday, forget it until Monday.** Three days of a local install with
> the pipeline released is about **$25**. With the App deployed as well it is
> about **$58**. Apps have no idle timeout and do not scale to zero, so
> `databricks apps stop` is a separate act from `./antidemo cleanup --yes`, which
> ends the rest of the installation's spend. The [README](../README.md) has the
> itemised figures.

The whole path is three commands:

```bash
cp docs/bootstrap.env.example .env.bootstrap   # fill in, never commit
./bootstrap.sh                                 # validate everything, provision nothing
./bootstrap.sh --apply                         # validate, show the bill, confirm, provision
```

**It installs and builds what it needs.** `uv sync --locked` provisions `.venv`
— which `./antidemo` refuses to run without — and `npm ci && npm run build`
provisions `frontend/dist`, which the UI answers 503 without. Both run before
anything is provisioned in the cloud, both are idempotent, and both are skipped
by the parts that have not changed: a second run reports
`frontend/node_modules is current` and `frontend/dist is newer than every source`
and moves on in about a second.

`--skip-install` (or `ANTI_DEMO_SKIP_INSTALL=1`) turns both off for someone who
has already done them and wants only the validation. `--print-env` implies it,
because a mode consumed by `eval` must not build anything as a side effect.

Nothing here ever runs a plain `uv run` or a bare `uv sync`. `--locked` installs
from the per-file URLs already in `uv.lock` and refuses to re-resolve; a
re-resolve is what once wrote an unreachable internal proxy hostname into all 775
of those URLs and cost 23 consecutive App deploys. See [Python
dependencies](#python-dependencies-and-the-one-command-that-needs-an-override).

`bootstrap.sh` refuses to start unless all nine of these are on `PATH`: `uv`,
`node`, `npm`, `databricks`, `aws`, `terraform`, `psql`, `python3`, `jq`. The
first seven are exactly the set `server/lifecycle.py:doctor` checks for.
`terraform` must be >= 1.9.0 (>= 1.11 only for the opt-in S3 state backend), and
Python must satisfy `requires-python = ">=3.12"`.

`.gitignore` already excludes `.env.*`, so `.env.bootstrap` cannot be committed.
The template lives at `docs/bootstrap.env.example` rather than
`.env.bootstrap.example` for the same reason: that name would have been ignored
too, and an uncommittable template is a template nobody finds.

With no `.env.bootstrap`, every missing value is prompted for instead; secrets
are read with `read -s` and never echoed. Every one of them is also read from the
environment, so `DATABRICKS_HOST=... ./bootstrap.sh --apply` works without a file.

A prompt with nobody to answer it is a **refusal, not a wait**. A `read` from
`/dev/tty` does not fail when the run is automated or supervised — it blocks for
ever, which presents as a hang with no output and no indication of which value is
wanted. So a missing input on a run with no usable terminal, or a prompt nobody
answers within `ANTI_DEMO_PROMPT_TIMEOUT_SECONDS` (default 120), exits non-zero
and names the variable, the file it belongs in, and the export that also works.

## The inputs

| Variable | Where it comes from |
|---|---|
| `DATABRICKS_HOST` | the workspace URL, `https://<host>`. Lakebase must be available on it |
| `DATABRICKS_CLIENT_ID` | the client ID of a workspace service principal's OAuth (M2M) secret |
| `DATABRICKS_CLIENT_SECRET` | the secret half of that same OAuth credential |
| `AWS_ACCESS_KEY_ID` | an IAM user with the policies in [`docs/iam/`](iam/README.md) |
| `AWS_SECRET_ACCESS_KEY` | the same |

`AWS_DEFAULT_REGION` is a sixth only when it has to be. When it is unset,
bootstrap reads `aws configure get region` — the region this laptop's AWS CLI is
already configured with, which is a fact about the machine rather than a decision
about the install — shape-checks it, and says where it came from. It is prompted
for only when there is no configured region to find.

### One optional seventh, and why it is not a seventh required input

`ANTI_DEMO_RUNTIME_PRINCIPAL_ARNS` is a comma-separated list of the exact IAM
user and role ARNs that should be allowed to assume a shared
[`anti-demo-runtime` role](iam/README.md#the-runtime-role-and-the-fortnightly-sweep).
Leave it unset — the default — and nothing changes: no role is created, and the
laptop and the deployed app each authenticate as themselves, which is why Round 5
cannot run in the deployed app.

Set it on a fresh install and one role is created that both principals assume, so
`sts:GetCallerIdentity` returns the same sealed identity either way. It is read
**at first provision only**. The ARNs Terraform actually creates are then sealed
into the manifest, and from that point the variable may only agree with the seal:
contradict it and `antidemo setup`, `antidemo doctor` and `antidemo renew` all refuse. This
is the same resolve-then-seal rule the Round 4 catalog follows, for the same
reason — an environment variable must not be able to silently re-decide something
an installation already committed to.

It adds no other input. The role's name is fixed, so its ARN is derivable before
Terraform runs; `ROUND5_APP_PRINCIPAL_ARN` is overridden with it automatically
rather than being something you supply.

A personal access token is not accepted and neither is `databricks auth login`
interactive state. The mechanism is OAuth M2M: `bootstrap.sh` writes a
`~/.databrickscfg` profile containing `host`, `client_id` and `client_secret`,
which is what both the Databricks CLI and the Python SDK read. That profile name
is then passed to every `antidemo` command.

## What it derives, so you do not have to know about it

| Derived | How |
|---|---|
| `AWS_EXPECTED_ACCOUNT_ID` | `sts:GetCallerIdentity` |
| `ROUND5_APP_PRINCIPAL_ARN` | the caller's own IAM user/role ARN; for an assumed role, resolved through `iam:GetRole`. Overridden with the runtime role's ARN when one is configured — see below |
| `ANTI_DEMO_MANIFEST` | the highest existing `.anti-demo-v*/manifest.json`, or a new `.anti-demo-v7` |
| `DATABRICKS_PROFILE` | `anti-demo-<workspace subdomain>`, written from the OAuth credentials |
| `DATABRICKS_WAREHOUSE_ID` | the workspace's only SQL warehouse; you are asked to choose if there are several |
| `DATABRICKS_APP_CLIENT_ID` | the Databricks App's service principal, by adopting or creating the app |
| `AWS_REGION` | mirrored from `AWS_DEFAULT_REGION`, because `server/cli.py` reads only the former |
| operator ingress `/32` | `checkip.amazonaws.com`, the same source `antidemo setup` uses |

`AWS_PROFILE` and `AWS_DEFAULT_PROFILE` are unset for the run. A named profile
and ambient keys together are refused outright by
`server/aws_auth.py:select_setup_auth`, and a stale `AWS_SESSION_TOKEN`
inherited from an SSO session is also cleared unless you supplied one.

## What it validates before spending anything

Nothing before the confirmation prompt provisions anything or costs anything. It
is not, however, a dry run of your laptop: the default mode writes two local
files, and it has to. See [what the default mode
writes](#what-the-default-mode-writes).

**Every check below reports, and the run stops once at the end.** A check that
fails records the failure and the run carries on; checks that genuinely depend on
a failed one are printed as `not checked:` rather than guessed at; and a single
gate — placed before the first byte is written into the generation directory and
before the cost summary is printed — reports everything that is wrong and exits.
This is deliberate. A first-failure-only gate turns one bad workspace into four
runs: fix the region, discover the warehouse is ambiguous, fix that, discover the
catalog does not exist. Failing on the first item and hiding the next has bitten
this project repeatedly.

Two things still exit immediately, because continuing would be dishonest rather
than merely noisy: a `~/.databrickscfg` profile that cannot be written (every
Databricks check after it would report failures it could not attribute), and the
Terraform state-backend guards, which are refusals to act rather than findings
about the environment.

1. All nine prerequisite binaries, including the seven `antidemo doctor` checks for.
2. `sts:GetCallerIdentity`, and that the account is 12 digits.
3. That a public IPv4 address is detectable — `detect_operator_cidr` rejects
   IPv6 and there is no fallback.
4. Fourteen read probes across RDS, EC2, Secrets Manager, SSM, CloudWatch and
   IAM, plus real EC2 authorisation dry runs for `CreateSecurityGroup` and
   `RunInstances`.
5. That a default VPC exists. `server/lifecycle.py:_terraform_variables` never
   passes `vpc_id`/`subnet_ids`/`runner_subnet_id`, and `infra/aws/locals.tf`
   requires all three together, so default-VPC discovery is the only supported
   network mode today.
6. That the service principal authenticates and that `databricks postgres
   list-projects` works — the same capability check
   `_verify_databricks_identity` performs.
7. That both Unity Catalogs exist: Round 6's `DATABRICKS_CDF_CATALOG` and Round
   4's `ROUND4_CATALOG`. Round 4's is checked in step 6 rather than step 5,
   because on an existing installation the manifest's sealed catalog outranks
   anything supplied and the manifest is not resolved until then.
8. On an existing installation, that the manifest's AWS account, region and
   Databricks principal all match what you supplied. `reconcile_infrastructure`
   refuses on any of those mismatches, and finding out here costs nothing.

`rds:Create*`, `iam:CreateRole` and `secretsmanager:CreateSecret` have no dry
run. The script says so rather than implying it proved them.

## What it does not do quietly

- **It never runs Terraform on its own.** The only mutating step is
  `./antidemo setup`, under `--apply`, after an itemised cost summary and a typed
  `PROVISION` confirmation.
- **It tells you when `antidemo setup` will be destructive.** On an existing
  installation `setup` runs `terraform plan` and `terraform apply` through
  `reconcile_infrastructure` (`server/lifecycle.py:5176`), so any pending diff
  in `infra/aws` is applied. The confirmation prompt says this explicitly.
- **It is resumable.** `antidemo setup` decides from the manifest whether to
  provision, resume an interrupted provision, or reconcile and reset a ready
  one, so re-running `bootstrap.sh --apply` after a failure continues instead of
  duplicating. `--apply` and `--deploy-only` record their derived values in
  `<manifest dir>/bootstrap.json`, mode 600, with no credentials in it; check
  mode writes nothing there.
- **It never prints or stores a secret.** Prompts use `read -s`; the
  `~/.databrickscfg` write happens in a subprocess that receives values through
  the environment, not `argv`; and a profile that already exists with different
  values is refused by name rather than by showing you either value.

## Re-running the installer where an installation already exists

**By default this is not a second install. It adopts the first one.** With no
`ANTI_DEMO_MANIFEST` set, bootstrap picks the highest-numbered existing
`.anti-demo-v<N>/manifest.json` and operates on it, so on a workspace that
already carries a live generation `./bootstrap.sh --apply` is not "install it
again" — it is `antidemo setup` against the running installation, which runs
`terraform plan` and `terraform apply` through `reconcile_infrastructure`,
applies any pending diff in `infra/aws`, resets both database lanes and clears
Round 3 anchors. Check mode now says that in a `warn` line at the point it adopts
the generation, and `--apply` repeats it in the confirmation prompt.

That is usually what you want — it is what makes a failed provision resumable —
but it is not what "run the installer again" sounds like.

**And on an installation whose manifest already reads `ready`, `--apply` refuses.**
There the reset is not a no-op: both database lanes are reseeded and the Round 3
anchors are cleared, so a bout in progress dies and every Round 3 recovery point
taken since the last reset is gone. The refusal names the two ways forward:

- `./bootstrap.sh --deploy-only` — republish the seal and redeploy the app. No
  database is touched and no Terraform runs. **This is the resume path for a
  ready install**, and it is what an operator reaching for `--apply` after a
  code change almost always meant.
- `./bootstrap.sh --apply --reset-ready` — the explicit opt-in, for an `infra/aws`
  diff to apply or lanes to return to a known state.

`--yes` does **not** authorise this. It suppresses the spend confirmation, and it
used to suppress the only sentence that mentioned the reset as well, so
`--apply --yes` against a ready install reset it silently. Any status other than
`ready` still falls straight through: that is the interrupted provision `--apply`
genuinely does resume.

`--new-generation` is the other behaviour: it provisions a fresh
`.anti-demo-v<N+1>` and leaves the existing generation, its Terraform state and
everything it owns untouched. **This is a second full fleet and a second full
bill.** The first one keeps billing until `./antidemo cleanup --yes` is run against
its own manifest; nothing about creating a second generation stops the first.

"Highest" is now numeric rather than lexical. The previous last-wins loop over
`.anti-demo-v*/` would have adopted `.anti-demo-v9` while `.anti-demo-v10` was
the live installation, because `v10` sorts before `v7` as a string — and then
reconciled a generation nobody was using while the real one kept billing beside
it. Nothing has reached double digits yet, so this was latent rather than live.

## What the default mode writes

"Validate everything, provision nothing" is about your cloud bill, not about
your filesystem. A default run writes one credential-bearing file, and it is not
optional:

- **`~/.databrickscfg`** gains an OAuth M2M profile section named
  `anti-demo-<workspace>`, or whatever `DATABRICKS_PROFILE` says, containing the
  service principal client ID and **secret** at mode 600. This is the only one
  worth knowing about, because it persists a credential. It is also load-bearing:
  every Databricks check after it — that the principal authenticates, that
  Lakebase answers, which warehouse exists, whether both catalogs exist, whether
  the app exists — runs as `databricks -p <profile>`, so a run that refused to
  write the profile would have to skip most of what it claims to validate. A
  section that already exists with different values is refused by name rather
  than overwritten; `--force-profile` overwrites it. A section that already
  matches is left untouched.

It is not the only thing check mode touches, and this document used to say it
was. A run that gets past the preflight gate also creates the generation
directory (`mkdir -p -m 700`) and opens `<manifest dir>/mutation.lock`, which is
how it refuses to read state another process is mid-way through rewriting; the
lock lives on this shell's open file descriptor and is released by the kernel
when the shell exits, however it exits. Neither is a credential and neither
belongs to an installation, but "nothing was written" was an overstatement.
A run that **fails** the preflight gate writes neither, because the gate is
before both. `--print-env` writes none of the three.

Check mode does **not** write `<manifest dir>/bootstrap.json`. It used to, which
made the read-only mode of the installer the thing that created an
installation's first file — and left an empty `.anti-demo-v<N>/` behind after a
run whose closing line says "Nothing was provisioned". Only `--apply` and
`--deploy-only` write it now.

If you want the validation without even the profile write, use `--print-env`: it
performs the same checks against a profile that already exists, says
`print-env mode: not touching ~/.databrickscfg`, and exits before any other
write.

### `<manifest dir>/bootstrap.json`

Written by `--apply` and `--deploy-only`, mode 600, the derived values with no
credentials in it. Nothing reads it; it is a record for a human, and for
answering "what was this installation pointed at" months later.

It is **merged, not overwritten**. `--deploy-only` never authenticates to AWS, so
the caller ARN, the Round 5 principal and the operator CIDR have no answer in
that mode, and it used to record its non-answers over whatever `--apply` had
resolved — leaving `"aws_caller_arn": "(not resolved: ...)"` and a malformed
`"operator_cidr": "/32"` in a file that had been correct. A run now records only
the values it actually resolved, and `recorded_by` names the mode that last
touched it.

## Python dependencies, and the one command that needs an override

Everything here follows from a single fact about uv: **it downloads from the
per-file URLs recorded in `uv.lock`, and consults an index only when it has to
resolve.** Installing is not resolving. So the commands split cleanly in two, and
only the second group needs anything special.

**These need no index and work everywhere, including on a laptop with no route to
public PyPI:**

```
uv sync                  # provision .venv
uv run --no-sync ...     # what ./antidemo uses for every subcommand
uv run pytest            # and uv run ruff check .
uv lock --check          # verifies the lockfile without re-resolving
```

There is nothing to configure for any of these and no flag to remember.
`pyproject.toml` declares the index as a property of the project, which is what
stops a machine-global `~/.config/uv/uv.toml` from deciding it instead.

**Only commands that genuinely resolve need an index** — `uv add`, `uv remove`,
and a bare `uv lock` after editing `[project.dependencies]`. On a machine that
cannot reach `pypi.org` these fail with `Connection refused`, and the fix is one
environment variable naming whatever index that machine *can* reach:

```
UV_DEFAULT_INDEX=<your reachable index> uv add <package>
```

**If that index is a mirror rather than public PyPI, you are not done.** uv writes
the resolved index's hostname into every URL in the lockfile — all 775 of them —
and a mirror only its own network can reach is what failed twenty-two consecutive
App deploys over three days. Normalise the hostnames straight back afterwards:

```
sed -i '' -e 's#https://<your-mirror>/packages/#https://files.pythonhosted.org/packages/#g' \
          -e 's#https://<your-mirror>/simple/#https://pypi.org/simple#g' uv.lock
uv run pytest tests/test_deploy_hygiene.py
```

This is safe when the mirror is a transparent one: the paths are identical, the
recorded `sha256` hashes are unchanged, and uv verifies every download against
them, so a substitution that was not byte-identical could not install at all.

You do not have to remember the last step.
`test_uv_lock_names_only_hosts_a_build_container_can_reach` fails on any hostname
that is not public PyPI, which turns a three-day deploy outage into a red test.
Do not silence it — the deploy is what it is standing in for.

### If you are on the maintainer's laptop

`pypi.org` is blackholed there: `/etc/hosts` maps it to `127.0.0.1`, along with
every other public package index, so it is refused in about a millisecond. This
is a policy blocklist, not an outage, and no amount of retrying will change it.
`files.pythonhosted.org` is **not** on that list and is fully reachable, which is
exactly why installs work while resolution does not. The only index reachable
there is a workspace-local package proxy, set as the default index by that
machine's global `~/.config/uv/uv.toml`; it is a transparent path-preserving
mirror of PyPI, so the two-step above applies verbatim.

## Deploying as a Databricks App

`bootstrap.sh --deploy-only` publishes the seal and deploys the app, and
`--apply --deploy-app` does it in the same run; **[docs/DEPLOY.md](DEPLOY.md) is
the reference for both** and supersedes anything below that disagrees with it.
The manual sequence is kept here because it is what the automation performs, and
because knowing it is what makes a failed deploy debuggable:

1. Build the frontend: `cd frontend && npm ci && npm run build`. `frontend/dist`
   is not committed and the app serves 503 without it.
2. Create four app resources on the app that `bootstrap.sh` created or adopted,
   with these exact resource keys, because `app.yaml` binds them by key:
   - `anti-demo-manifest-json` — the **contents** of the sealed manifest, as a
     secret. This is the one people get wrong: `app.yaml` binds
     `ANTI_DEMO_MANIFEST_JSON` to the secret, not to the file, so the secret has
     to be rewritten and the app restarted every time setup or resume changes
     the seal. `antidemo renew` prints this reminder; `antidemo setup` does not.
   - `aws-access-key-id`, `aws-secret-access-key`, `aws-session-token` — the
     same AWS credentials. All three bindings are required to *resolve*, because
     Databricks Apps has no optional binding: an `env` entry is a `name` plus
     either `value` or `valueFrom`, and a `valueFrom` naming a resource that does
     not exist fails the app at startup with `error resolving resource`. A
     keys-only installation therefore creates `aws-session-token` as a secret
     holding the **empty string**, which both `botocore` and this repository read
     as "no session token". See [the session token](#the-aws-session-token).
3. Grant the app's service principal: `CAN_USE` on the warehouse, `SELECT`/
   `MODIFY` on the Round 4 source table, read on the target Postgres, and
   connect plus table privileges on `anti_demo_coordination.ring_lease`. README
   has the full list.
4. Sync the working tree and deploy the app.

Reading the manifest from that secret is not gated on expiry, and there is no
longer any code that could gate it: the refusing `assert_not_expired` has been
deleted from `DemoManifest`, and `_warn_if_expired` and `_expiry_check` are both
advisory. The app does fail closed if the selected round's seal is incomplete:
v5 for Round 5, live-validated v6 for Round 6.

## The AWS credential the deployed app runs on

The app authenticates as whatever `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
you put in `.env.bootstrap`. Those exact values are published into the app's
Databricks secrets and bound by `app.yaml`. There is no second credential, no
role assumption and nothing minted on your behalf.

**Supply a permanent IAM key pair, not a session.** This is the single decision
that determines whether the deployed app keeps working. Databricks Apps resolves
a `valueFrom` secret once, at container start, and the process holds that value
for its whole life — a rotated secret does not reach a running container. So:

| What you supply | What the app does |
|---|---|
| Permanent IAM user key (`AKIA…`, no session token) | works indefinitely; nothing to re-authenticate, ever |
| STS session (`ASIA…` plus a token) | works until the session expires, then Rounds 1, 2, 3 and 5 leave the card while 4 and 6 keep working |

The second row is not hypothetical. On 2026-08-24 the deployed app was found
serving as `assumed-role/AWSReservedSSO_…`, published from an `aws sso login`
that had leaked in through the environment. It worked that afternoon and was
dead the next morning. `bootstrap.sh` now **refuses** to publish a temporary
credential into the app and names the two variables to fix; `--i-know-this-expires`
overrides it if a short-lived credential really is what you want.

Nothing on the install path requires `aws sso login`. A plain access key and
secret from an IAM user is accepted end to end.

### What the deployed app does with the key

The app holds whatever the supplied key holds, so it is worth knowing that
**provisioning and running are two different privilege sets**:

- **Provisioning** genuinely needs the broad set — it creates IAM roles,
  security groups and an EC2 instance. That is the three-document operator set
  in [`docs/iam/`](iam/README.md).
- **The deployed app** needs far less: RDS catalog reads, point-in-time restores
  into the `adsc-*` and `adrc-*` per-bout prefixes, deletes scoped to those same
  prefixes, `secretsmanager:GetSecretValue` on `rds!*`, and two metric reads.
  That is [`docs/iam/anti-demo-app-runtime.json`](iam/anti-demo-app-runtime.json),
  and `python -m server.aws_permissions` prints the same set derived from the
  round modules' own call sites rather than from a list anybody maintains.

If you supply one key for both, it must hold the broad set, and the deployed app
will hold the broad set too — including `iam:CreateRole` and `ec2:RunInstances`.
That is the simple path and it works. If you would rather the app held less,
create a second IAM user on `anti-demo-app-runtime.json`, and put *that* user's
key in `.env.bootstrap` before running `--deploy-only`. Two consequences to know
before you do:

- **Provisioning must still run as the broad key.** Run `--apply` with it, then
  swap the two AWS values in `.env.bootstrap` and run `--deploy-only`.
- **Round 5 will report `principal_mismatch`.** Its control role's trust policy
  seals exactly one principal, sealed at provision from whoever provisioned. An
  app authenticating as a different principal is not that one, so `/readyz`
  reads `degraded` with Round 5 off the card and the other five unaffected.
  On a **default** install nothing is lost that was working, because Round 5 is
  not on a default install's deployed card in any case; the health surface just
  says `degraded` rather than `ready`. On an install that sealed a shared runtime
  role, this split *does* cost you a working round — see
  [Default limit: Round 5 is not on a default install's card](#default-limit-round-5-is-not-on-a-default-installs-card).
  **The same applies to a local `./antidemo serve`,** which authenticates
  as whatever `.env.bootstrap` or the shell supplies: seal the installation to one
  of the two principals and the other loses Round 5 locally too. The mismatch is
  reported and served through rather than refused — see
  [what a mismatch does at launch](#what-a-round-5-principal-mismatch-does-at-launch).

Using a single key for both avoids that entirely: the provisioning principal and
the app principal are then the same ARN, `principal_matches` agrees, and
`/readyz` reads `credentials_state: ok`.

## Default limit: Round 5 is not on a default install's card

**This is intended, it is not a broken install, and there is nothing to wait for
or grant.** Five rounds on the card instead of six is the correct outcome for a
default install, and the app says so itself — Round 5's catalog entry carries
the reason in full, headed:

> THIS ROUND IS NOT ON TONIGHT'S CARD. Round 5 is run by the operator, from the
> operator's own machine, as the one identity its controls were sealed to.

Round 5 assumes a control role whose trust policy names **exactly one**
principal, fixed when the installation was sealed, and a default install seals no
shared role for the app and the operator to reach it through. Nothing running
inside the app can edit a sealed trust policy, so the app withholds the round.

**The refusal is one thing rather than two, and the difference matters if you are
reading an older copy of this page.** `docs/iam/anti-demo-app-runtime.json` grants
`sts:AssumeRole` on `role/*-r5-exec-*` — the Round 5 execution role and nothing
else in the account. Earlier revisions said the app principal held no
`sts:AssumeRole` at all and was therefore refused twice over; that is no longer
true. What refuses the round in the deployed app is the shared trust, and it is
enforced in code rather than only by IAM: `server/round_availability.py` withholds
Round 5 from a deployed app whenever no runtime role is sealed, whatever the
grant says.

Run Round 5 from a local checkout, attended, as the principal that provisioned:

```bash
./antidemo serve      # then drive Round 5 from localhost:8000
```

The mechanism that puts Round 5 on the deployed card exists, is switched off by
default, and **has been exercised**: `anti_demo_runtime_principal_arns` in
`infra/aws/variables.tf` creates a shared `anti-demo-runtime` role trusted by
both the operator's principal and the app's, and makes *that* role the control
role's single trusted principal. The deployed app then reaches its runner in two
STS hops — ambient credentials into the runtime role, runtime role into the
sealed execution role — rather than having to be the trusted principal itself.
An installation configured this way has declared four Round 5 bouts from the
deployed app, both lanes verified and `cleanup_failure` null on each; the
[README](../README.md#what-has-been-proven-and-what-has-not) has the figures and
the receipt codes.

Two conditions come with it, and neither is optional. The variable defaults to
`[]`, and because the trust policy is sealed at first provision, turning it on is
a **fresh install rather than a repair**. And a trusted role does not open a
security group: the deployed app also needs the Databricks serverless egress
prefixes sealed into the manifest and applied to the database security groups,
or all four AWS-backed rounds are refused at the network instead. Unless you need
Round 5 in front of a remote audience, leaving it off is still the right call.

### What `/readyz` says about this, and why not to read too much into it

An install in exactly this correct state reports `status: degraded` and
`credentials_state: principal_mismatch`. Both are accurate and neither is an
error. `degraded` here means "fewer rounds than the maximum are available", which
for a default install is the permanent, intended steady state. Judge the install
by whether the five rounds are `ready` in `/api/catalog`, not by the one word in
`/readyz`.

## Serve-time credentials: the same file, carried to `./antidemo serve`

`bootstrap.sh` reads `.env.bootstrap`; nothing else used to. That gap meant a
first-time user could complete `./bootstrap.sh --apply`, follow its closing
advice to run `./antidemo serve`, and get a server with no AWS credentials at
all — quietly, because the missing credential degrades to four rounds off the
card rather than to a refusal.

The `antidemo` launcher now reads the same file, and the rule is:

> **The environment wins. Only a variable that is unset or empty right now is
> filled from the file.**

Two things follow, both deliberate. A credential you exported yourself is never
overridden. And an **empty** value in the file cannot clobber a good one in the
environment — which matters, because leaving both AWS key fields blank is a
supported configuration: it is how you run `--deploy-only` without republishing
whatever AWS credential your shell happens to be carrying.

Only `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`,
`AWS_REGION` and `AWS_DEFAULT_REGION` are carried. Everything else in the file
is the manifest's business at run time; a launcher that also exported
`ANTI_DEMO_MANIFEST` could silently point a serve at another generation.

Set `ANTI_DEMO_ENV_FILE` to read from somewhere other than `.env.bootstrap` in
the repository root.

A serve that ends up with no AWS credentials prints a block naming the file and
the two variables, and says which four rounds it just lost. It is not fatal —
Rounds 4 and 6 reach Lakebase and no AWS, and they genuinely work — because the
bug was never the degrade, it was the silence. Confirm from the server rather
than from the absence of the banner:

```bash
curl -s localhost:8000/readyz | jq .credentials_state   # want "ok"
```

### SSO versus static keys

No step in this repository requires `aws sso login`, and no code path looks for
an SSO profile. Fill the two AWS fields in `.env.bootstrap` with a permanent key
pair and `./antidemo serve` needs no browser login, ever — it carries those keys
into the server itself.

Two caveats, both worth knowing before you rely on that:

- **`antidemo doctor` is not covered by it.** Doctor makes reads the narrow app
  runtime policy does not grant — `ec2:DescribeSecurityGroups` in particular — so
  on an installation whose `.env.bootstrap` holds an app-runtime key, doctor has
  to be run from a shell carrying broader credentials. `antidemo status` is
  unaffected and runs fine on the narrow key.
- **Which principal the server authenticates as decides Round 5.** See below.

### What a Round 5 principal mismatch does at launch

`antidemo serve` compares the principal it resolved against the one sealed in
`round5.control_role_trusted_principal_arn`, and on a mismatch it **prints a
warning and serves anyway**. That is deliberate and was changed on purpose: it
used to exit 1 and refuse to serve at all, which cost all six rounds to protect
one. The mismatch is already handled downstream — the credential probe reports
`principal_mismatch`, `/readyz` degrades, and `server/round_availability.py`
withholds Round 5 with the reason attached — so the round is never advertised and
cannot die at the bell.

What you see, in order: a stderr block beginning `!! SERVING WITHOUT ROUND 5`, a
`/readyz` reading `credentials_state: principal_mismatch` with
`credentials_principal` naming whoever you really are, and five rounds instead of
six in `/api/catalog`. The repair is a restart under the sealed principal. The
credential probe's five-minute re-ask cannot help, because a running process's
environment does not change.

## The AWS session token

`app.yaml` binds `AWS_SESSION_TOKEN` to the `aws-session-token` app resource
unconditionally, and it has to: Databricks Apps supports no optional `env`
binding, and a `valueFrom` whose resource is missing fails the app at startup
rather than leaving the variable unset. Removing the entry would break every
SSO-credentialled deployment, where the token is real and required.

An empty secret is the supported way to say "there is no token here":

- `botocore.credentials.EnvProvider` reads the token with `if token:`, so an
  empty `AWS_SESSION_TOKEN` is not passed to any AWS call.
- `server/aws_auth.py:_present` is `bool(environment.get(name, ""))`, so the
  same value is absent for `_validate_key_shape` and is not forwarded to
  subprocesses by `selected_subprocess_environment`.

Whitespace is not equivalent: a single space is truthy to both, and would be
signed into requests as a bogus token.

`bootstrap.sh --deploy-app` already does this for you: when `AWS_SESSION_TOKEN`
is unset it writes the resource as an empty value, on stdin like every other
secret it publishes, so no credential ever appears in `argv`. By hand, the
equivalent is `printf '' | databricks secrets put-secret SCOPE
aws-session-token`.

## Round 4's Unity Catalog

`ROUND4_CATALOG` selects it, exactly as `DATABRICKS_CDF_CATALOG` selects Round
6's. The default in `server/lifecycle.py:ROUND4_DEFAULT_CATALOG` is `main`, the
catalog Databricks itself creates when a workspace is enabled for Unity Catalog,
so it is the one name with a real chance of already existing in a workspace this
repository has never seen. That makes it a *likely* default, not a guaranteed
one: where `main` is absent or invisible to the principal, the installer refuses
before any Unity Catalog write and names `ROUND4_CATALOG` as the variable to set.
Round 4 deliberately does not create the catalog itself, because cleanup deletes
only the three schemas it made and has no catalog delete — a created catalog
would be an orphan nothing reaps.

The variable is read **only on a first provision**.
`server/lifecycle.py:_round4_catalog` then prefers the manifest's sealed
`round4.storage_catalog`, because the catalog reaches `CREATE SCHEMA`, `GRANT`
and cleanup `DELETE SCHEMA` statements: a later run that fell back to a
compiled-in default would act on a catalog it did not provision. A value that
contradicts the seal is refused by name rather than silently ignored or silently
obeyed, and `bootstrap.sh` refuses the same mismatch before spending anything.

## What needs more than the five inputs

Stated plainly, because "five inputs" is the claim this document leads with and
these are the exceptions to it.

**`ROUND4_CATALOG` is required on any workspace without a usable `main`.** Round 4
needs a Unity Catalog it can create a schema in, and the compiled-in default
(`server/lifecycle.py:ROUND4_DEFAULT_CATALOG`) is `main` — the catalog Databricks
creates for a Unity Catalog-enabled workspace, so on most workspaces there is
nothing to set. It is a likely default rather than a certain one, and the
installer does not gamble on it: a run that falls back to it says in as many
words that nobody chose it for this workspace and that it is sealed on first
provision, and if `main` is absent or invisible to the principal the run refuses
early — before any Unity Catalog write and before anything is spent — naming
`ROUND4_CATALOG` as the variable to set and the `databricks catalogs list` that
shows the candidates. An **existing installation is unaffected either way**: the
catalog sealed into the manifest outranks both the variable and the default, so a
generation provisioned into some other catalog keeps working untouched and a
variable that contradicts the seal is refused rather than obeyed. It is read on a
first provision only and then sealed — see
[Round 4's Unity Catalog](#round-4s-unity-catalog).

**`DATABRICKS_WAREHOUSE_ID` is required when the workspace has more than one SQL
warehouse.** With exactly one, bootstrap selects it and prints its ID *and its
name*. With several, it prints every candidate's ID, name, type and state, and
gives the line to paste into `.env.bootstrap`. Round 4 seals the choice into its
contract, so it refuses to guess. An API failure from `databricks warehouses
list` is now reported as an API failure: it used to fall back to an empty list
and report "no SQL warehouse is visible", which sent operators off to create a
warehouse they already had.

**`ROUND5_APP_PRINCIPAL_ARN` is derived in the common case and required in one.**
`sts:GetCallerIdentity` returns a stable ARN directly for an IAM user or role,
and bootstrap adopts that string verbatim — it is not an input for anybody using
long-lived keys, which is what `docs/bootstrap.env.example` asks for. For an
assumed role it is resolved through `iam:GetRole`, and when *that* fails it stays
required and is not guessed at. That refusal is deliberate and worth being
explicit about: Round 5's control-role trust policy is generated from this value
and sealed into the manifest at first provision, so a wrong ARN provisions
perfectly and then fails Round 5 at click time, in front of an audience. There is
no string transformation from the STS `assumed-role` form to the IAM form — the
path is dropped and cannot be recovered — so the only safe options are the
authoritative lookup or an explicit value. The error now prints the exact
`aws iam list-roles` query that produces the answer.

**That hazard is confined to first provision, and on a sealed installation the
variable cannot cause it at all.** Once a manifest exists, the sealed value
outranks the environment: `server/lifecycle.py` resolves the principal from the
seal and a contradicting `ROUND5_APP_PRINCIPAL_ARN` is refused by `antidemo
setup`, `doctor` and `renew` rather than quietly re-deciding anything. On an
installation that seals a shared runtime role the variable is not consulted for
the trust policy in the first place — the runtime role's ARN is, and bootstrap
overrides the variable with it automatically. Earlier revisions of this page
described the variable as a standing pinning hazard for the life of an install.
It is not; it is a first-provision input like the others.

**Round 4 also requires `DATABRICKS_APP_CLIENT_ID`, which bootstrap derives** by
creating or adopting the Databricks App. Setup seals that exact service principal
into the Round 4 contract and cannot publish a v2 manifest without it. Driving
`./antidemo` without bootstrap means supplying it yourself.

**Grants on the app's service principal are manual.** The deploy cannot grant them
to itself. [docs/DEPLOY.md](DEPLOY.md#coordination-database-grants--the-complete-runtime-set)
has the runnable statements.

**Terraform state is local.** `infra/aws/versions.tf` declares
`backend "local" {}`, and `_terraform_init` points it at
`<manifest dir>/terraform.tfstate`. That is a deliberate property, not an
oversight: the manifest and the state are one generation, kept together in a
gitignored directory, and `antidemo cleanup` compares manifest tags against live AWS
tags. It does mean the state file is a local artifact that must not be lost — if
it is, the resources still exist and bill, and only the ownership tags identify
them. Two people cannot bootstrap the same installation from two laptops.

An opt-in S3 backend is available: see
[docs/DEPLOY.md](DEPLOY.md#terraform-state-in-s3) for `--state-backend s3`, the
three `ANTI_DEMO_TF_*` variables and the fourth IAM policy. `_terraform_init` now
derives the backend from a per-generation `terraform-backend.json`, so
`bootstrap.sh` no longer refuses it. Local remains the default and is what every
existing installation uses — a generation with no backend record behaves exactly
as before. The S3 path is opt-in, new installations only, and no `terraform init`
has yet been run against it; DEPLOY.md is precise about what that leaves
unverified.

## The databases are reachable from the internet

Every database this installs is created with a public endpoint. All four Aurora
Serverless v2 writers and all three RDS PostgreSQL instances set
`publicly_accessible = true` — `infra/aws/aurora.tf:45` and `:97`,
`infra/aws/rds.tf:28` and `:70` — so each one has a public DNS name and a public
IPv4 address, and each of those addresses is separately billable. That is a
design decision, not a default that slipped through, and it is stated here
because a stranger cloning this repository provisions it in their own account.

### What actually restricts access

One security group per database — on a current install that is four Aurora and
three RDS, from the `for_each` blocks `aurora_by_round` and `rds_by_round` in
`infra/aws/network.tf`, plus two `count`-gated blocks that only exist on a legacy
single-pair install. All four blocks are the same shape, and the whole inbound
surface is TCP 5432:

| Rule | Source | Where in `network.tf` |
|---|---|---|
| PostgreSQL 5432 | `var.operator_cidr` — your public `/32` | `251-257` and `302-308` (legacy: `99-105`, `151-157`) |
| PostgreSQL 5432 | `var.serverless_egress_cidrs` — the Databricks-published serverless egress prefixes, when sealed | the same blocks, admitted by `concat()` on the one `cidr_blocks` list |
| PostgreSQL 5432 | security-group *reference* to the Round 5 runner, Round 5 only | `259-268` and `310-319` (legacy: `107-113`, `159-165`) |

There is no other port, no other protocol, and **no `0.0.0.0/0` inbound rule
anywhere**. Egress is open (`-1` to `0.0.0.0/0`, e.g. `network.tf:270-276`),
which is what carries stateful responses and AWS service traffic. The runner's
own group has **no ingress rules at all** (`network.tf:185-192`) — it is
outbound-only, and it reaches the databases by being named as a source in theirs.

Two constraints are enforced rather than advised. `operator_cidr` must be an
explicit `/32` — `variables.tf:71-82` rejects every other netmask — and
`serverless_egress_cidrs` must be distinct `/24`-or-narrower prefixes, with
`variables.tf:90-96` refusing a `/16` in front of a live database in the same
words `server/manifest.py` does. The ingress blocks are **inline** on purpose: an
inline `ingress` makes Terraform authoritative over the group's entire rule set,
so a rule added by hand or by another tool is revoked on the next apply. Moving
to standalone rule resources would quietly turn that seal from enforced into
advisory.

### What that does and does not buy you

`publicly_accessible = true` behind a `/32` is not an open database. It is also
not a private one, and the difference is worth being exact about:

- **The protection is one security-group rule.** There is no VPC boundary behind
  it. Widen that rule — by accident, by a debugging session nobody reverted, or
  by a tool that manages security groups — and what is behind it is PostgreSQL
  password authentication facing the internet directly.
- **The Databricks-published prefixes are not tenant-isolated.** Databricks says
  so plainly: outbound IPs are shared across customers. Sealing them admits any
  Databricks serverless workload in that region to port 5432, where it still
  meets password auth. For throwaway demo databases that is a reasonable trade;
  for anything holding real data it is not.
- **This may breach your own organisation's policy.** Plenty of corporate AWS
  accounts prohibit public database endpoints outright, and a config rule or SCP
  may flag or block this. Settle that before `--apply`.

Keep nothing in these databases you would mind losing or exposing. They are
seeded with synthetic demo data, they are destroyed by `antidemo cleanup`, and in
the sandbox this was built in they are deleted wholesale every fortnight.

### Why not private

The demo has to be driven from two places that are both outside the VPC: an
operator's laptop, and a Databricks App running on serverless compute. A private
endpoint reaches neither without an NCC with PrivateLink, which is an
Enterprise-plan construct requiring an **account administrator** — something a
stranger cloning this cannot be assumed to have, and the reason this project does
not require one anywhere.

It was priced rather than dismissed. All seven lanes speak 5432 and PrivateLink
preserves the client port, so one listener cannot disambiguate them and the
straightforward build is seven NLBs: at the us-west-2 list rate of `$0.0225/hour`
that is **+$3.78/day** in load-balancer hours alone, before NLCUs and before
Databricks private-connectivity data transfer — several times the entire public
IPv4 bill it would replace. It also does not simplify the guard it was hoped to
simplify: Databricks requires inbound-rule enforcement **off** on the endpoint
service's NLB, and on that setting AWS's own guidance is to authorise by private
IP rather than by security-group reference, so the ingress allowlist has to
accept more than one CIDR either way.

### If this posture is not acceptable to you

There is no supported private mode, and this is the honest answer rather than a
pointer to a flag that does not exist. The options are to run it in a disposable
account where a public endpoint is acceptable, or to build the PrivateLink path
yourself — in which case `_postgres_ingress_is_exact` in `server/lifecycle.py`
and the four security groups in `infra/aws/network.tf` are the two places that
decide what a valid ingress set is, and both would need to change together.

## When your IP changes under a long-lived install

The database security groups allow exactly one address on your behalf: the public
/32 this host had at `antidemo setup`. A DHCP lease, a VPN toggle or a different
network moves it, and from that moment every round that connects directly to
Aurora or RDS fails to connect. Nothing on the demo screen says so, which is what
made this the likeliest silent killer on an install left up for days.

(An install that runs AWS-backed rounds from the *deployed* app seals a second
set alongside it — the four Databricks-published serverless egress prefixes for
the workspace's region, applied to the same security groups. Those are the
vendor's published ranges and do not move with your laptop, so nothing below
applies to them. Your `/32` is still the only address that carries a locally run
server, and losing it still costs you every direct round locally.)

Two commands name it, both read-only:

```bash
./antidemo status               # advisory 'operator_ingress' line, cached probe
./antidemo doctor               # 'operator_cidr' line, probes directly
```

`/readyz` is the third surface and the one a monitor would watch. The detector it
needs is in place (`server.lifecycle.operator_ingress_drift_async`, which folds
into the existing `degraded` / `degraded_detail` / `degraded_capabilities`
vocabulary), but the `app.py` wiring has not landed — that file was held by
another change. Until it does, `/readyz` says nothing about ingress.

The fix is `./antidemo setup`, which re-detects the address, rewrites the manifest
and re-applies the security groups. It is the only thing that performs the
repair, deliberately: the serving process never mutates, so it cannot rewrite a
security group on a bad inference and cannot corrupt a running measurement. The
detector is cached (five minutes; thirty seconds after a failed probe), is
short-timeout, and treats an unreachable network or an IPv6-only one as "unknown"
rather than as drift — a false positive here sends an operator to re-apply
Terraform for nothing.

## Stopping the spend

```bash
./antidemo cleanup --dry-run    # inventory only
./antidemo cleanup --yes        # destroy manifest-owned resources
```

The `expires-at` tag is an ownership label. Nothing reaps on it. The default TTL
is 72 hours (`server/lifecycle.py:DEFAULT_TTL_HOURS`, and `ANTI_DEMO_TTL_HOURS`
overrides it), a passed expiry is a `WARN` line rather than a failure, and
`antidemo renew --ttl-hours N` moves it.

That default describes a *new* provision only. The TTL is written once, at
`created_at + ttl_hours`, and is never re-based, so an existing installation
carries whatever value it was provisioned with — a live manifest showing a 24-hour
window is not a disagreement with this default, it is an installation that was
provisioned with `--ttl-hours 24`. Read `expires_at` from the manifest, never
`created_at + DEFAULT_TTL_HOURS`.

Nothing enforces it either way. There is no code path that refuses on a passed
`expires_at`: the method that once did (`DemoManifest.assert_not_expired`) raised
`RuntimeError` into callers that swallowed `RuntimeError`, which silently removed
Round 5 from a running installation with no log line naming the cause. It has
been deleted, and `tests/test_expiry_renew.py` fails if it comes back. The
supported treatment is `DemoManifest.expiry_warning()`, which reports and returns.
