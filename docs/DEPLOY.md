# Remote Terraform state, and deploying the Databricks App

Two opt-in additions to `./bootstrap.sh`. Neither changes the behaviour of an
existing installation unless you ask it to.

- [Terraform state in S3](#terraform-state-in-s3) — opt-in, new installations only
- [Deploying the app](#deploying-the-databricks-app) — automated, including the
  secret rewrite and restart that everyone forgets
- [Required patch to `server/lifecycle.py`](#required-patch-to-serverlifecyclepy)
- [What is untested](#what-is-untested)

---

## Terraform state in S3

### The default has not changed

`infra/aws/versions.tf` still declares `backend "local" {}`, and with no flags
bootstrap still puts state in the generation directory next to the manifest.
That is not laziness: `antidemo cleanup` reconciles manifest tags against live AWS
tags, so keeping the manifest and the state together makes a generation one
self-describing unit. The cost is that the state is a local artifact you must
not lose, and that two people cannot drive one installation.

### How the two modes coexist

Terraform will not let a configuration declare two backends, and will not let a
backend block interpolate a variable. Partial configuration (`-backend-config`)
can fill in a backend's *arguments* but cannot choose its *type*. So the type
has to come from a file, and the only mechanism that can replace an existing
`backend` block is an override file.

`_terraform_init` generates `infra/aws/backend_override.tf` when — and only
when — the generation being operated on has a `terraform-backend.json`, and
deletes it otherwise. `infra/aws/backend_override.tf.example` is the reference
shape, and the generated name is gitignored.

The alternative was to delete the backend block from `versions.tf` and always
generate one. That is fewer moving parts, but it makes a fresh clone with no
generated file fall back to Terraform's implicit local backend rooted in
`infra/aws/`, which is not where any generation keeps its state. Preferring an
override keeps the committed default both explicit and correct.

Regenerating from the generation's own record on every init is the part that
matters. A flag you can forget is a flag that eventually re-inits an S3-backed
installation against an empty local state — at which point `plan` proposes
creating a second copy of every billed resource, and the first copy becomes
invisible.

### Locking: S3-native, no DynamoDB

`use_lockfile = true`. S3-native state locking arrived in Terraform 1.10 and
went GA in 1.11, and DynamoDB-based locking is now deprecated with removal
announced. Requiring 1.11 for this path buys real locking with no lock table:
no second billed resource, no extra IAM surface, and nothing for `antidemo cleanup`
to fail to destroy.

`versions.tf` keeps `required_version = ">= 1.9.0"`. Raising the floor for
everyone would have been a breaking change for local-backend installs that this
feature has no business making, so bootstrap enforces the 1.11 floor only when
`--state-backend s3` is passed.

### Bootstrap creates the bucket

Recommended, and implemented that way. The argument for requiring a pre-existing
bucket is less privilege — `s3:CreateBucket` is a real grant. The argument for
creating it is that this file is the only record of which billed resources you
own, and a hand-made bucket is how you end up with state in an unversioned one.
Bootstrap creates it with versioning enabled, SSE-S3 default encryption and all
four public-access blocks set, and **fails the run if versioning cannot be
enabled** rather than proceeding with an unrecoverable state file.

If your cloud team would rather pre-create it, drop
`CreateAndHardenTheStateBucket` from the policy below and create the bucket with
those same three properties. Bootstrap detects an existing bucket, reports its
region, and warns if versioning is off.

### Using it

The `_terraform_init` patch this depends on has landed in `server/lifecycle.py`,
so `bootstrap.sh` no longer refuses `--state-backend s3`. What that buys is the
path being *open*, not the path being *exercised*: no `terraform init` has ever
run against the generated override, and the three refusals that remain still
apply. See [What is untested](#what-is-untested).

```bash
./bootstrap.sh --state-backend s3 --state-bucket my-tfstate-bucket          # validate
./bootstrap.sh --state-backend s3 --state-bucket my-tfstate-bucket --apply  # provision
```

or in `.env.bootstrap`:

```bash
ANTI_DEMO_TF_BACKEND=s3
ANTI_DEMO_TF_STATE_BUCKET=my-tfstate-bucket
# ANTI_DEMO_TF_STATE_KEY=anti-demo/.anti-demo-v<n>/terraform.tfstate
```

The key defaults to `anti-demo/<generation directory>/terraform.tfstate`, keyed
on the generation rather than on `run_id`, because `run_id` changes on every
reset.

### Four refusals, before anything is created

1. **The generation already exists.** Switching a live installation's backend is
   `terraform init -migrate-state`, which rewrites the only record of what you
   own. Bootstrap will not do it. See below.
2. **Terraform is older than 1.11.**
3. **`server/lifecycle.py` has not been patched.** Satisfied now, and still
   checked: bootstrap greps this tree for `terraform-backend.json`, so a revert
   or a partial checkout closes the path again rather than letting `antidemo setup`
   fail at init. Without the patch, `_terraform_init` passes
   `-backend-config=path=...`, a local-backend argument the S3 backend rejects
   outright.
4. **The bucket name is not a legal S3 name**, or `s3:ListAllMyBuckets` is
   denied.

### Migrating an existing installation — by hand, deliberately

Not automated, and not something to do while a demo is scheduled. Terraform is
moving the file that says what you own; if it goes wrong, the resources still
exist and still bill, and only their ownership tags identify them.

> [!WARNING]
> **This sequence cannot be completed as written, and step 3 is the dangerous
> half of it.** Step 4 runs `terraform init` directly, and at that point
> `infra/aws/versions.tf` still declares `backend "local" {}`. A backend *type*
> cannot come from `-backend-config`, only its arguments can, so the S3 type has
> to come from `infra/aws/backend_override.tf` — and the only thing that writes
> that file is `_terraform_init` in `server/lifecycle.py` (1154-1186), whose four
> call sites all either apply or destroy: `_complete_provision` (7113),
> `reconcile_infrastructure` (7434), `_renew_locked` (8007) and `cleanup` (9494).
> `doctor` (8797) is not among them. So no read-only command in this tree arms
> the override for a hand-run `init`, and `backend_override.tf.example` explicitly
> forbids creating it by hand. What is missing is a read-only step that reads
> `terraform-backend.json`, writes or removes `backend_override.tf`, and exits
> without touching AWS.
>
> Step 3 writes `terraform-backend.json` *before* the migration is proven, and
> that record is what `antidemo cleanup` reads. If step 4 then fails, cleanup
> initialises against an S3 key holding no state while the real state sits in the
> local file it never reads. It no longer reports a clean teardown on that: the
> empty-state branch (9557-9604) cross-checks the account by tag and refuses
> under `--yes`. But that cross-check is account-side only, so it reaches exactly
> as far as the tag read does — residue that is untagged, reads as `retiring`, or
> sits outside the manifest's region is not caught by it. The local
> `terraform.tfstate` the manifest already names settles the same question with no
> API call, and cleanup should refuse when the initialised backend yields an empty
> state while a non-empty local state exists for that generation. That is the same
> class as the state-derived teardown blindness the empty-state branch was written
> for, reached by a second route. Do not write the record until the state has
> landed.
>
> Until this is closed in code, stay on the local backend for an existing
> installation. `--state-backend s3` on a *new* install is the supported path.

```bash
# 0. Name the generation once, from bootstrap rather than from memory. No
#    generation number is written down in this document: it increments on every
#    re-provision, and the names do not sort in numeric order.
eval "$(./bootstrap.sh --print-env)"    # ANTI_DEMO_MANIFEST and the rest; no secret
GEN=$(dirname "$ANTI_DEMO_MANIFEST")    # the .anti-demo-v<n>/ you are migrating
KEY="anti-demo/$(basename "$GEN")/terraform.tfstate"

# 1. Copy the state somewhere outside the tree. Not a rename -- a copy. The
#    leading dot is stripped so the backup is not a hidden file in your home.
cp "$GEN/terraform.tfstate" ~/"${GEN##*/.}-state-$(date +%s).json"

# 2. Create the bucket with versioning, SSE and public access blocked.
#    ./bootstrap.sh --state-backend s3 --state-bucket NAME --apply does this on
#    a new install; for a migration, do it by hand or with the CLI.

# 3. Write the generation's backend record.
cat > "$GEN/terraform-backend.json" <<JSON
{ "backend": "s3", "bucket": "NAME",
  "key": "$KEY",
  "region": "REGION", "use_lockfile": true, "encrypt": true }
JSON

# 4. Migrate. Read the prompt; it names both backends. Answer "yes" only if the
#    source is the local path you expect.
terraform -chdir=infra/aws init -migrate-state \
  -backend-config=bucket=NAME \
  -backend-config="key=$KEY" \
  -backend-config=region=REGION \
  -backend-config=use_lockfile=true

# 5. Prove it moved nothing. This must be an empty plan -- but neither command
#    below does that as written. See the two notes under this block.
./antidemo doctor
terraform -chdir=infra/aws plan -detailed-exitcode   # exit 0 = no changes
```

If step 5 shows a diff, stop. Do not apply. The state did not land as expected,
and the backup from step 1 is the way back.

Two things step 5 does not do, both of which have to be fixed before this
sequence is trustworthy:

- **`antidemo doctor` runs no Terraform.** It checks that the `terraform` binary
  is on `PATH` and nothing further, so it cannot say whether state moved. It is
  also not read-only — it restores the sealed Round 4 baseline and wakes Lakebase
  out of scale zero — which makes it the wrong thing to run in the middle of a
  state migration.
- **The bare `plan` is missing every variable the configuration requires.**
  `infra/aws/variables.tf` declares seven with no default — `aws_region`,
  `aws_account_id`, `run_id`, `owner`, `expires_at`, `operator_cidr` and
  `round5_app_principal_arn`. With no `-var` arguments and no `-input=false`,
  Terraform prompts for all seven, and any value that differs from the seal
  renders as a diff that is not state drift. `_terraform_plan` supplies these
  from the manifest; a hand-run `plan` has no such source.

### IAM

`docs/iam/anti-demo-operator-4-state.json` — five statements, 1059 characters,
against the 6144-character limit. It is a fourth file rather than a merge into
the existing three: the permissions are only needed on the opt-in path, and
keeping them separate means the default install grants nothing for S3 at all.
Merging is also safe — folding it into the largest of the three reaches 5249 of
the limit — but it grants S3 access to installations that will never use it.

It was drafted with four statements and 927 characters, under a name
(`iam-addendum-s3-state.json`) that was never committed — the draft was adopted
into the file above rather than added beside it, so there is nothing to open
here and nothing to compare against. Adopting it split one statement in two,
because as drafted the
policy did not match the first of the three notes below: `s3:DeleteObject` sat
alongside `GetObject` and `PutObject` on a resource list naming both the state
object and the lock, which grants delete on the state object as well.

Three notes for a reviewing cloud team, spelled out in
[`docs/iam/README.md`](iam/README.md#4--terraform-state-in-s3-opt-in):

- **`s3:DeleteObject` is scoped to `*.tflock`, not the state object.** Terraform
  never deletes state; it does delete its lock. Granting delete on the state
  object would let a bad actor erase your only record of ownership. It is its own
  statement, `DeleteOnlyTheLock`, with one ARN.
- **`s3:ListBucket` is conditioned on the `anti-demo/*` prefix**, so this
  principal cannot enumerate an unrelated bucket it happens to be pointed at.
- **`s3:ListAllMyBuckets` is account-wide** and cannot be scoped — that is the
  API's shape. It reveals bucket *names* only. It is used by one preflight probe;
  drop it if your team objects, and the probe will report as denied while
  everything else still works.

---

## Deploying the Databricks App

```bash
./bootstrap.sh --deploy-only          # publish the seal, deploy, restart, verify
./bootstrap.sh --apply --deploy-app   # provision and deploy in one run
./bootstrap.sh                        # reports drift, deploys nothing (writes a local profile)
```

`--deploy-only` never touches AWS and never runs Terraform. It skips the
permissions preflight, the state-backend decision and the bill.

### How the deployed app gets AWS credentials, and what breaks them

The app requires **an access key and a secret in its process environment**, and
nothing narrower than that. `server/aws_auth.py:validate_app_aws_environment`
pins `AWS_AUTH_MODE=environment`; `validate_runtime_auth` then demands
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` both non-empty and refuses if
`AWS_PROFILE` or `AWS_DEFAULT_PROFILE` is set; `APP_AWS_BINDINGS` adds
`AWS_REGION`, `AWS_EXPECTED_ACCOUNT_ID`, `AURORA_CLUSTER_ID` and
`AURORA_SECRET_ARN`; the expected account is shape-checked to twelve digits and
then compared against a live `sts:GetCallerIdentity`. There is no keyless
branch, and `_CONFLICTING_CREDENTIAL_NAMES` strips the web-identity variables a
federated path would need — so a *keyless* federated path is genuinely closed.

**A temporary STS session satisfies every one of those requirements.**
`AWS_SESSION_TOKEN` is optional, not forbidden and not required:
`_validate_key_shape` constrains it in one direction only — a token without a
key pair is refused, a key pair without a token is fine, and a key pair *with* a
token is equally fine. Nothing in the path inspects the access key's prefix, and
nothing anywhere reads an expiry time; `selected_subprocess_environment`
deliberately *forwards* `AWS_SESSION_TOKEN` into anything it spawns, and
`aws_credential_probe.principal_matches` treats an `assumed-role/...` caller as a
first-class case, compared by role name. Nor is this only theoretical: the deploy
publishes the operator's real token whenever this run has one (`bootstrap.sh`,
the `aws-session-token` case), and a live deploy has done exactly that — the app
came up on a session credential and reported an assumed-role principal to its own
`/readyz`, then inventoried the account and found all twelve sealed resources.

So the binding constraint is **presence, not staticness**. The real limitation is
**expiry**: a session credential stops working at a fixed time, the scope's
secret values cannot be read back to discover when, and no renewal path is
decided. `bootstrap.sh` says so itself at the end of its publish loop — a deploy
that supplies no keys warns that whatever the scope holds may be a long-expired
SSO session and that the AWS lanes will fail while Lakebase rounds keep working.
Until something renews it, the deployed app's AWS reach is time-boxed by
whichever credential was published last.

**A credential the app cannot use no longer stops it from booting.** It used to:
`validate_app_aws_environment` raised out of the FastAPI lifespan on the first
attempt — `AwsAuthConfigurationError` is not a transient coordination error, so
the startup retry did not apply — and the container never started. That made the
sweep fatal in a way no credential design fixes, because `EphemeralNuke` deletes
the IAM users along with the databases: a *running* process survives the sweep,
having validated once at boot, and the next restart after it came up with nothing
at all — including Rounds 4 and 6, which reach Lakebase and no AWS whatsoever.
The check is unchanged and still runs in the same place; what changed is that the
answer is now reported instead of thrown. A refused credential comes up as
`credentials_state` `rejected` (or `absent`, when nothing is exported at all) on
`/readyz`, with the diagnosis in `credentials_detail` and every lost lane named in
`degraded_capabilities`; `ring_ready` is untouched, so the app keeps its 200 and
stays in rotation, and the refusal is logged at error level with its traceback.
This is deliberately the same end state a *running* replica reaches when its
credentials die under it, which means the credential probe's 5-minute re-ask
applies: publish a working key and the app goes green with no restart. **`antidemo
serve` is unaffected and still refuses outright** — an operator is present there,
is about to spend money, and a hard refusal on a laptop is the point.

The Databricks-native alternatives were investigated and found unavailable
rather than merely awkward; that investigation is recorded in the maintainer's
notes and is not published with this repository. On a fresh install that asks for
it, laptop and app both assume a single
[`anti-demo-runtime` role](iam/README.md#the-runtime-role-and-the-fortnightly-sweep)
and the control role trusts that role instead. It costs $0, and it is what makes
Round 5 reachable from the deployed app — see
[Round 5 from the deployed app](#round-5-from-the-deployed-app-the-two-hop-assume)
below.

**The operator half of that needs no application code change; the app half does.**
A local `~/.aws/config` profile carrying `role_arn` and `source_profile` is
resolved by botocore transparently, `auth_mode: profile` sets only `AWS_PROFILE`,
and `principal_matches` compares the resulting `assumed-role/.../<session>` by
role name correctly — so the laptop reaches the runtime role with configuration
alone. The deployed app cannot use that route: `validate_app_aws_environment`
pins `AWS_AUTH_MODE=environment`, `validate_runtime_auth` **refuses the
credential** if `AWS_PROFILE` or `AWS_DEFAULT_PROFILE` is set, `session_arguments` passes no
`profile_name` in that mode, and `selected_subprocess_environment` strips
`AWS_CONFIG_FILE`, `AWS_SHARED_CREDENTIALS_FILE` and `AWS_SDK_LOAD_CONFIG` from
anything it spawns. Even given a config file, botocore resolves environment
credentials ahead of a profile's assume-role provider, so the app would keep
authenticating as whatever those credentials name.

And that is the point: the app authenticates as whatever principal the published
key, secret and optional token resolve to — which is why a session credential
works at all, and equally why nothing in the app's own configuration *determines*
that principal.

### Round 5 from the deployed app: the two-hop assume

**The fix was to stop requiring the app to be the sealed principal at all.**
Making the app's ambient identity deterministic was one way to close this and it
is not the way it was closed. On an installation that seals a shared runtime
role, the app's ambient principal stays incidental *and stops mattering*, because
it is no longer what the control role trusts.

The deployed path is two STS hops:

1. The app's ambient credentials — whatever the last deploy published — assume
   the shared `anti-demo-runtime` role. Those credentials need only
   `sts:AssumeRole` on that one role.
2. The runtime role assumes Round 5's sealed control role, which trusts the
   runtime role and nothing else.

`sts:GetCallerIdentity` then returns the same sealed identity from the laptop and
from the container, which is the property the seal actually checks. The operator
half needs no application code: a local `~/.aws/config` profile with `role_arn`
and `source_profile` gets there, as described above. The app half is the second
hop, made explicitly in `server/connection_spike_live.py` rather than through
botocore profile resolution — which the deployed app cannot use, for the reasons
in the preceding paragraphs.

**This has been run.** Four Round 5 bouts have been declared from the deployed
app on an installation configured this way, both lanes verified and
`cleanup_failure` null on each; the
[README](../README.md#what-has-been-proven-and-what-has-not) has the figures and
the receipt codes.

**Two conditions, and neither is optional.**

- `ANTI_DEMO_RUNTIME_PRINCIPAL_ARNS` is read **at first provision only**, because
  the control role's trust policy is sealed then. Adding it to an existing
  installation is a fresh install, not a repair.
- A trusted role does not open a security group. The deployed app also races
  Aurora and RDS over TCP 5432, so the Databricks serverless egress prefixes have
  to be sealed and applied to the database security groups too. Without them
  `server/round_availability.py` refuses all four AWS-backed rounds at the
  network — Round 5 included, whatever the trust says.

`server/round_availability.py` reports the runtime-role refusal *before* the
network one, deliberately: the network path is an ordinary re-seal and the
runtime role can only be sealed at first provision, so an operator told about the
re-seal first would do it, restart, and find Round 5 still refused.

#### Redeploying a Round 5-capable app: refresh the manifest secret, not the source

This is the part with operational teeth. `app.yaml` binds the seal to a
**secret's contents**, so redeploying source alone leaves the app matching a
stale seal — and a stale seal is not inert here. The app will compare it, decide
Round 5 is available, **advertise the round as ready on the card**, and then fail
an assume-role *after the bell*. The round is promised to the room and then dies.

So a Round 5-capable redeploy is `./bootstrap.sh --deploy-only`, which
republishes the seal and redeploys together — not a source-only push. Run
`./bootstrap.sh` with no flags first if you want the drift reported before you
spend anything; it is the check described in
[The stale seal is the real problem](#the-stale-seal-is-the-real-problem) below,
and on a Round 5-capable install it is the difference between a stale seal caught
on a laptop and one caught in front of an audience.

**The fortnightly cost, in a swept account.** The AWS sandbox this was developed
in deletes IAM users on a roughly 14-day sweep; your own account will not unless
you have arranged something similar. Where it does happen it is expensive here.
Because a trust policy stores a user's *unique principal ID* rather than
its ARN, recreating the user with the same name does not restore the
relationship — and nothing that compares names can tell. After each sweep:
recreate the user, push new keys into the `aws-access-key-id` and
`aws-secret-access-key` app resources, restart the app, then `antidemo doctor` and
`antidemo renew`. The full procedure, including what each step proves, is
[in the IAM README](iam/README.md#the-sweep-ran-bring-it-back).

### The stale seal is the real problem

`app.yaml` binds `ANTI_DEMO_MANIFEST_JSON` to the **contents** of a secret, not
to a path. The deployed app therefore serves whatever seal was last pushed, and
keeps serving it. Every `antidemo setup`, every resume, every `antidemo renew` rewrites
the manifest — and the deployed app does not notice. `antidemo renew` prints a
reminder; `antidemo setup` does not.

So `./bootstrap.sh` with no flags now reports it:

```
==> 8/9  Databricks App state
  ok    app 'lakebase-anti-demo' exists: compute ACTIVE, deployment SUCCEEDED
  ok    the app reads its seal from scope 'lakebase-anti-demo-ad-...'
  drift the deployed seal is stale
  warn  the manifest has changed since the secret was last pushed.
        pushed  sha256:1f3c9a...
        current sha256:8b20de...
```

**How it detects that, and the limitation.** The secret value cannot be read
back: `secrets get-secret` is an explicitly undocumented DBUtils-only API that
refuses ordinary callers outside a notebook. So there is no way to hash what the
app is actually holding. Instead:

- A local record, `<manifest dir>/app-deploy.json` (mode 600), stores the
  sha256 of the manifest at the moment it was pushed. Comparing that against the
  manifest now catches every local change — which is every case where `setup`,
  `resume` or `renew` moved the seal.
- The remote `last_updated_timestamp` from `secrets list-secrets` is compared
  against the one recorded at push time. A mismatch means somebody pushed from
  somewhere else; the contents cannot be verified, so it reports drift and asks
  you to republish.
- No local record at all is treated as drifted, because a hand-deployed app's
  seal is unknowable.

Nothing here can prove a *matching* seal byte-for-byte. It can prove a
mismatched one, and it fails toward "republish", which is the cheap direction.

### What the deploy does

1. **Refuses if the seal is not servable.** `app.py:108` raises
   `InvalidStateError` inside the FastAPI lifespan when the manifest status is
   not `ready`, and `app.py:112` when the seal is older than v2. That raise means
   the container never starts — while the deploy still reports success. Checking
   it locally first costs nothing and is the single most useful guard here.
2. **Refuses if `frontend/dist` is missing**, which makes the app answer 503 on
   every page. It does not build for you: `npm run build` overwrites the
   directory a locally running server is serving from.
3. **Reads the required resource keys out of `app.yaml`.** Whatever `valueFrom:`
   names, the deploy binds — the list is not duplicated in `bootstrap.sh`, so
   adding or removing a binding there needs no change here. A key it does not
   recognise is reported as something you must create by hand rather than
   silently skipped.

   `aws-session-token` is published with this run's real token when it has one,
   and as the empty string when it does not. The resource has to exist either
   way, because Databricks Apps has no optional binding and a `valueFrom`
   pointing at a missing resource fails the app at startup — so "required, but
   allowed to be empty" describes the *binding*, not the credential: empty is
   what both botocore (`if token:`) and `server/aws_auth.py` (`bool()`) read as
   absent. `app.yaml`'s own comment covers why empty and not a space.
4. **Creates the secret scope if absent**, tolerating a concurrent create.
5. **Writes each secret**, values on stdin so no credential is ever in `argv`
   where `ps` could see it. `put-secret` overwrites in place, which is what makes
   the whole path idempotent.
6. **Rebinds the app's resources** with `apps update --json`. Resources can only
   be set as a whole list, so the list is rebuilt rather than patched.
7. **Syncs the tree and deploys**, excluding `.anti-demo*`, `.venv`,
   `node_modules`, `.git` and `.env*`.
8. **Stops and starts the app.** A new deployment restarts it anyway; the
   explicit restart covers the secret-changed-but-source-identical case, which is
   the common one, and costs a minute.
9. **Verifies it actually came up**, below.

### Verifying it started

Polls `apps get` for up to ten minutes, and succeeds only on
`compute_status.state == ACTIVE` **and** `active_deployment.status.state ==
SUCCEEDED`. It breaks early on `ERROR`, `STOPPED` or a `FAILED` deployment
instead of waiting out the timeout.

On failure it prints `compute_status.message`, the deployment message, and the
last 60 lines of `databricks apps logs` — then names the specific failure this
app has actually had, so you do not have to recognise it in a stack trace:

```
An InvalidStateError above means the seal in the secret is not servable:
app.py raises inside the FastAPI lifespan when the manifest status is not
READY, and the container never finishes starting.
```

It exits non-zero in that case. A deploy that reports success while the
container crashes is the outcome this is built to prevent.

### Limits of the deployed runtime

**An armed fight card does not survive the app process.** The coordination ring,
the cost ledger and the bout receipts are durable in `anti_demo_coordination`; an
armed fight card is not. It is a live object graph — round engines, asyncio
tasks, and the SSE event log — in the memory of the one process that armed it. A
graceful restart releases the ring on the way out, so a redeploy costs the fight
card but leaves the round immediately re-armable. An *ungraceful* loss — SIGKILL,
an OOM kill, a container eviction — leaves the armed lease on the ring with no
process able to ring the bell, and the ring stays fenced for the remainder of
`ANTI_DEMO_ARM_TTL_SECONDS` (180 s by default). The control routes name that
condition and print the countdown rather than returning a bare 404, but the bout
is lost and must be prepared again.

**Do not enable horizontal scaling for this app, and do not raise `--workers`.**
Databricks Apps horizontal scaling (opt-in Beta, 1–5 instances) provides only
best-effort session affinity, and affinity is dropped on deployment, on a crash,
on an instance-count change, and on rebalancing. Either change lets an `arm` and
its `run` land on different processes, which produces the failure above on demand
rather than only on a crash. The app is deliberately a single instance running a
single uvicorn worker; the durable ring makes that safe against a second
*installation*, not against a second process serving the same one.

### Which Python the app installs on

Databricks Apps picks its installer from which files are in the deployed tree,
and that choice picks the interpreter:

| deployed tree contains | installer | interpreter |
| --- | --- | --- |
| `requirements.txt` | pip | 3.11, regardless of `pyproject.toml` |
| `pyproject.toml` + `uv.lock`, no `requirements.txt` | uv | whatever satisfies `requires-python` |

This repository ships `pyproject.toml` and `uv.lock` and **no**
`requirements.txt`. That is not an omission — it is the mechanism. The file used
to exist, and its presence alone put the deployed app on Python 3.11, where
`server/connection_spike_journal.py`'s PEP 695 `type` alias does not parse. The
result was a `SyntaxError` at import, a container that never served a request,
and `apps get` reporting compute `ACTIVE` with the deployment `SUCCEEDED`
throughout. `bootstrap.sh` used to work around it by withholding the file from
the sync; deleting it closes the hole for anyone who deploys without
`bootstrap.sh` too.

Three things keep it closed, because a file this consequential should not depend
on nobody re-creating it by accident:

- `bootstrap.sh` refuses to run at all while `requirements.txt` exists.
- `.gitignore` lists it, so `git add -A` cannot bring it back.
- `tests/test_deploy_hygiene.py` asserts its absence, asserts `uv.lock` is
  present and agrees with `pyproject.toml`, and — the check that would have
  caught the original failure before it shipped — parses every `.py` file in the
  tree against the *oldest* interpreter `requires-python` admits.

An `--exclude` keeps a file from being uploaded; it does not delete a copy an
earlier sync already placed in the workspace, and the runtime keys off mere
presence. So every deploy asks the workspace directly whether
`<source>/requirements.txt` exists and deletes it if so. That check is
deliberately **not** conditional on a local `requirements.txt`, because the case
it exists for is precisely the one where there is no local file to test:
an installation last deployed before the file was removed.

### Why nothing pins an interpreter

There is no `.python-version` in this repository and no `UV_PYTHON` in
`app.yaml`, and both absences are deliberate.

A root `.python-version` cannot be used even though the deploy would tolerate
one: uv reads it and discards a project environment built on any other
interpreter. A stray one pinning 3.12 deleted this tree's provisioned 3.14
`.venv` and began rebuilding it, which is indistinguishable from a hang, and it
did so during an incident — presenting as a stalled recovery. `bootstrap.sh`,
`.gitignore` and `tests/test_deploy_hygiene.py` all refuse it now, the same three
layers as `requirements.txt`.

`requires-python = ">=3.12"` is the entire correctness requirement, and it is
satisfied by anything uv can pick: `uv.lock` carries wheels for `cp312` through
`cp315`, so every candidate interpreter can both install and parse this source.
Pinning one anyway would mean naming an interpreter the build image may not have,
and uv's answer to that is to fetch a managed one — more traffic through the
least reliable component in the build. That trade is only worth making with
evidence, and the evidence does not exist yet; the next section says exactly what
would produce it.

### The package proxy timeouts — SETTLED 2026-08-22, and it was not the proxy

> **This whole section was wrong about the cause, and the error is instructive.**
> The install failures — twenty-two consecutive over three days — were not an
> outage. `uv.lock` pinned all 719 wheel URLs and 53 registry entries to
> a workspace-local package proxy — an internal mirror reachable only from the
> maintainer's network, which the Apps build container has no route to. That
> mirror is healthy and answers `200`
> from this laptop; that was never the question. It got into the lockfile from a
> machine-global `~/.config/uv/uv.toml` that sets that index as `default = true`
> and is **not in this repository**. The fix was to re-point the lockfile at
> public PyPI, which installs byte-identical wheels — the proxy is a transparent
> path-preserving mirror, verified by full sha256 comparison and by all 719
> recorded sizes matching `files.pythonhosted.org`. Zero version drift.
>
> **The claim below that "the proxy is not an optional mirror; it is the index" is
> corrected.** It was a true observation about the lockfile turned into a false
> constraint on the build, because nobody asked *why* the lockfile named that
> host. Once it was written down as settled, every later investigation assumed
> the proxy had to work and went looking at interpreters, concurrency and
> timeouts instead. It was an optional mirror all along, and re-pointing it was
> two string replacements.
>
> The full account, evidence and method are in the maintainer's notes, which are
> not published with this repository. What matters here: the interpreter
> hypothesis below is dead, and the four observables are superseded.
>
> **Observable 4 is ANSWERED, by the deploy that fixed this.** `app.yaml`'s `env`
> block *does* reach the dependency-install step: the armed `UV_COMPILE_BYTECODE`
> probe fired, `Bytecode compiled 1278 files in 2.35s`, and uv prints that line
> only if it saw the variable. So `UV_CONCURRENT_DOWNLOADS` and `UV_HTTP_TIMEOUT`
> were reaching uv the whole time. They were never inert — they were
> **irrelevant**, because no timeout or concurrency setting reaches an unroutable
> host. The question that gated this work for three days turns out not to have
> mattered. Keep the probe as a positive control.
>
> **A trap for anyone reproducing the fix on the maintainer's laptop:** `uv run`
> and `uv sync` re-resolve by default, so an ordinary `uv run pytest` rewrites
> `uv.lock` straight back to the internal mirror. That happened once during this
> investigation, between the fix and the deploy, and the deploy shipped the
> un-fixed file. `pyproject.toml` now declares the index explicitly, which is
> what makes the correction survive. Do not remove that block.
>
> One correction to the section below on interpreters: the build image does
> **not** supply CPython. uv fetches a managed 3.14.3 into `/app/.uv-python/`
> already, so "pinning would make uv fetch a managed interpreter" describes the
> status quo rather than a new cost.

Six consecutive deploys failed with `client error (Connect): operation timed out`
against the workspace package proxy, a different wheel each time, which rules out
a dependency-resolution problem. `app.yaml` responds by lowering
`UV_CONCURRENT_DOWNLOADS` to 4 and raising `UV_HTTP_TIMEOUT` to 180, and
`bootstrap.sh` retries a transport-shaped failure up to `DEPLOY_ATTEMPTS=4`.

The standing hypothesis is that `requires-python >= 3.12` lets uv resolve **3.14**
and therefore request `cp314` wheels the proxy has probably not cached, where an
older and more widely cached target would sail through. **This cannot be settled
from the repository**, and it is worth being precise about why, because the
lockfile narrows it a long way:

- ~~`uv.lock` resolves every package against an internal package proxy, with
  per-file URLs and hashes. The proxy is not an optional mirror; it is the
  index.~~ **CORRECTED.** The first half was the bug, written down and not
  recognised. The lockfile named that host only because it was resolved on a
  machine whose global uv config made it the default index; it was never a
  property of this project or of Databricks Apps. `uv.lock` now names
  `files.pythonhosted.org` for every file and `https://pypi.org/simple` as the
  registry, with every recorded hash unchanged.
- All 54 packages have wheels. None is sdist-only, so nothing is compiled during
  the build and no timeout can be blamed on a source build.
- The two `resolution-markers` (`python_full_version >= '3.14'` and `< '3.14'`)
  produce **identical package versions**. No package appears, disappears or
  changes version across the 3.14 boundary.
- Only 12 packages carry interpreter-specific wheels at all: `cffi`,
  `charset-normalizer`, `cryptography`, `httptools`, `protobuf`,
  `psycopg-binary`, `pydantic-core`, `pynacl`, `pyyaml`, `uvloop`, `watchfiles`,
  `websockets`. The other 42 are pure-Python or `abi3` and resolve to the same URL
  on every interpreter. `cryptography` and `pynacl` reduce further, since their
  `abi3` wheels cover everything from 3.9 and 3.8 respectively.

So the interpreter choice changes roughly **ten of fifty-four download URLs**, and
which interpreter uv picks is a property of the Apps build image — recorded
nowhere in this repository, this lockfile, or these docs.

That makes the hypothesis sharply falsifiable. One live deploy attempt settles
it, provided the build log is read for these four things:

1. **The interpreter uv chose.** uv logs it as `Using CPython 3.x.y`, and whether
   it says `interpreter at ...` (found in the image) or shows a download
   (`Downloading cpython-3.x.y`) matters as much as the version.
2. **The wheel filename in the timeout line**, not just the package name. If the
   failures land on the ten interpreter-specific wheels, the hypothesis holds. If
   any lands on a pure-Python wheel — `fastapi`, `starlette`, `boto3`,
   `databricks-sdk`, `botocore` — the interpreter is irrelevant and the proxy is
   simply unreliable under load.
3. **Whether the same wheel ever fails twice** across the four attempts. A
   different file every time points at concurrency or per-connection limits; the
   same file every time points at a genuinely absent cache entry.
4. **Whether `UV_CONCURRENT_DOWNLOADS` and `UV_HTTP_TIMEOUT` reached uv at all.**
   Everything above assumes `app.yaml`'s `env` is visible to the install step and
   not only to the running process. That assumption is inherited, never verified,
   and if it is wrong then the mitigation already in place is doing nothing and
   the timeouts have a much simpler explanation. uv echoes neither, so the
   observable proxy is timing: four concurrent downloads and a 180-second
   patience produce a visibly slower build than fifty and thirty seconds, and a
   timeout that fires at ~30s rather than ~180s is the tell.

Only (4) is worth a deploy on its own. If `app.yaml` `env` does not reach the
build, the fix is a different lever entirely and nothing about interpreters
matters yet.

### Scope naming

Default: `lakebase-anti-demo-<generation directory>`, keyed on the generation,
which is stable across resets. The existing hand-made convention embeds `run_id`,
which changes on every reset and forces the app's resource bindings to be
rewritten alongside the secret. If the app is already bound to a different
scope, bootstrap adopts that one rather than orphaning it. Override with
`ANTI_DEMO_SECRET_SCOPE`.

### Grants the deploy cannot issue itself

The deploy writes secrets, binds resources and pushes source; it cannot grant
privileges to its own service principal. `antidemo setup` does that instead, one
provision before the app's first read — the Databricks-side half is `CAN_USE` on
the warehouse, `USE CATALOG` / `USE SCHEMA` / `SELECT` / `MODIFY` on the Round 4
source table, and read on the Round 4 target Postgres. The two sections below are
that whole set written out: the Unity Catalog and Lakebase grants first, then the
Postgres coordination grants in full. Both blocks are for running by hand only
against an installation provisioned before the fix that added them.

### Unity Catalog and Lakebase grants — what the deployed app reads

`antidemo setup` issues all of these, from `_round4_unity_catalog_grants()`,
`_round6_unity_catalog_grants()` and `_grant_lakebase_app_project_use()` in
`server/lifecycle.py`. Run them by hand only against an installation provisioned
before that fix — and note that re-running setup rescues only half of one. Round
4's grants sit in `_ensure_round4`, which resume calls unconditionally, so they
reapply. Round 6's sit in `prepare_round6`, and resume returns early on
`round6_ready`, so a sealed Round 6 never sees them again.

Two of them were missing for a release, and both symptoms were the deployed app
dying at **arm** on its very first control-plane read while the same round ran
fine from a local checkout as the operator:

- **Round 4** — `PermissionDenied: User does not have SELECT on Table
  '<catalog>.<online schema>.model_scores'`. The plan granted `SELECT, MODIFY`
  on `<catalog>.<source schema>.model_scores_source` and stopped there. That is
  a real table, and the wrong one: `inspect_sync` GETs
  `/api/2.0/database/synced_tables/<synced table id>`, which Unity Catalog
  authorizes as `SELECT` on the **synced** table. The app had `USE SCHEMA` on
  the online schema and nothing on the one table inside it. `USE SCHEMA` is
  traversal only and never implies `SELECT`.
- **Round 6** — `PermissionDenied: The user is not authorized to make the
  request, please contact the workspace admin to assign the user <app>
  'Can Use' or 'Can Manage' for Database project <project uid>`. Setup created
  the app's Lakebase OAuth role on the branch, which authenticates a Postgres
  connection and authorizes nothing on the control plane. Every
  `/api/2.0/postgres/...` read is checked against the project's own ACL.

```sql
-- Round 4, on the warehouse the manifest seals. The synced table is the entry
-- that was missing; it is a different securable from the source table.
GRANT SELECT ON TABLE `<catalog>`.`<online schema>`.`model_scores`
  TO `<app-client-id>`;
```

```bash
# One per Lakebase project the app touches -- Round 4's, Round 6's, and the
# coordination project. CAN_USE, never CAN_MANAGE: the other level the API
# offers carries the ability to delete the project. `update` rather than `set`,
# so the operator's own CAN_MANAGE entry survives.
databricks permissions update database-projects <project-id> --json '{
  "access_control_list": [
    {"service_principal_name": "<app-client-id>", "permission_level": "CAN_USE"}
  ]
}'
```

The permissions endpoint is keyed on the project **ID** (`<install>-r6`), not on
the project UID the refusal message quotes; the UID comes back in `object_id`.

Round 6 also needs one Postgres privilege on its own source table, applied on
the Round 6 branch endpoint as the schema owner:

```sql
-- The round commits a proof order through checkout and then settles by
-- removing that exact row. Granting only SELECT and INSERT leaves one row of
-- drift per bout, and the bout still reports `verified` -- the only signal is
-- `Round 6 settlement attempt n/4 failed ... InsufficientPrivilege: permission
-- denied for table live_orders` in the app log. No UPDATE: a proof row is
-- written once and withdrawn, never amended.
GRANT SELECT, INSERT, DELETE ON TABLE
  "<round 6 source schema>"."live_orders" TO "<app-client-id>";
```

### Coordination-database grants — the complete runtime set

**The deployed app is a consumer of `anti_demo_coordination`, not a co-owner.**
It gets no `CREATE`, on the database or on the schema. `antidemo setup` provisions
every object here as the operator's own identity; the app only reads and writes
rows. `server/cost_ledger.py` and `server/readiness.py` verify each object is
present at startup and refuse to serve if one is not, so a missing grant is a
loud failure rather than an empty ledger.

Run these against database `anti_demo` on the **coordination** endpoint, as the
identity that owns the schema. `<app-client-id>` is the app's
`service_principal_client_id` (`databricks apps get <app> | jq -r
.service_principal_client_id`) — the same value setup seals as
`DATABRICKS_APP_CLIENT_ID`.

```sql
GRANT CONNECT ON DATABASE anti_demo TO "<app-client-id>";
GRANT USAGE   ON SCHEMA   anti_demo_coordination TO "<app-client-id>";

-- Ring lease. Claim is INSERT ... ON CONFLICT DO UPDATE, which needs both
-- halves; renew and release are UPDATE; `current()` and the staleness
-- diagnosis are SELECT.
GRANT SELECT, INSERT, UPDATE ON anti_demo_coordination.ring_lease
  TO "<app-client-id>";

-- Startup readiness row. Same INSERT ... ON CONFLICT DO UPDATE shape.
GRANT SELECT, INSERT, UPDATE ON anti_demo_coordination.startup_readiness
  TO "<app-client-id>";

-- Round 5 creation journal: append-only, so no UPDATE and no DELETE.
GRANT SELECT, INSERT ON anti_demo_coordination.round5_creation_journal
  TO "<app-client-id>";
-- `event_id` is bigserial, so an INSERT that omits it draws from the sequence.
GRANT USAGE, SELECT ON SEQUENCE
  anti_demo_coordination.round5_creation_journal_event_id_seq
  TO "<app-client-id>";

-- Cost ledger (v7). Estimates are INSERT; `close_bout` and `reconcile_window`
-- are UPDATE; every read path is SELECT. `SELECT ... FOR UPDATE` needs the
-- UPDATE privilege too, which the same grant supplies.
GRANT SELECT, INSERT, UPDATE ON anti_demo_coordination.cost_ledger
  TO "<app-client-id>";
-- Snapshots are an immutable revision history: written once, never amended.
GRANT SELECT, INSERT ON anti_demo_coordination.cost_reconciliation_snapshot
  TO "<app-client-id>";
-- The calibration profile is recomputed rather than amended, so this one needs
-- DELETE: `reconcile_window` deletes the row for the affected key and INSERTs a
-- freshly aggregated one. It is the only DELETE the app holds anywhere.
GRANT SELECT, INSERT, DELETE ON anti_demo_coordination.cost_calibration_profile
  TO "<app-client-id>";

-- Bout receipt history: append-only, so no UPDATE and no DELETE. A later
-- terminal event for an already-declared bout is a new row that loses the
-- read, never an overwrite.
GRANT SELECT, INSERT ON anti_demo_coordination.bout_receipt
  TO "<app-client-id>";

-- Round 4 pipeline power history: which deliberate stops and starts this
-- installation has made. Append-only for the same reason as the journal above,
-- so no UPDATE and no DELETE. The app writes here because it now starts the
-- pipeline at arm and stops it once a bout has settled, and the local marker
-- file those verbs write cannot exist in a Databricks App -- the manifest
-- arrives as an environment variable, so there is no directory to sit beside.
-- Without this row a stop the app made is indistinguishable from a pipeline
-- that fell over, which is the one thing the record is for.
GRANT SELECT, INSERT ON anti_demo_coordination.round4_pipeline_power
  TO "<app-client-id>";
-- `event_id` is bigserial, so an INSERT that omits it draws from the sequence.
GRANT USAGE, SELECT ON SEQUENCE
  anti_demo_coordination.round4_pipeline_power_event_id_seq
  TO "<app-client-id>";
```

That is the whole set. Three things deliberately absent, so nobody adds them
back looking for a fix:

- **No `CREATE` on `DATABASE anti_demo`.** This is what
  `psycopg.errors.InsufficientPrivilege: permission denied for database
  anti_demo` was asking for. It is not the fix — the schema already exists, and
  `CREATE SCHEMA IF NOT EXISTS` fails on the ACL check, which Postgres runs
  *before* the existence check. The app now checks `pg_catalog` first and skips
  the statement.
- **No `CREATE` on `SCHEMA anti_demo_coordination`.** Same reasoning one level
  down: creating a table inside an existing schema needs this rather than the
  database privilege, and the app never needs to.
- **No grant on `anti_demo_coordination.protect_cost_estimate()`.** Postgres
  checks `EXECUTE` on a trigger function when the trigger is *created*, not when
  it fires, and `CREATE FUNCTION` grants `EXECUTE` to `PUBLIC` by default.

`server/lifecycle.py` issues **all** of these during `_ensure_round4`, from the
single plan in `_coordination_runtime_grants()`. It used to issue only five of
them — `CONNECT`, `USAGE`, the ring lease, the journal and the journal's
sequence — and the earlier version of this paragraph called those "the first
five of these", which read as though `startup_readiness` (listed *fourth* in
the block above) was among them. It was not: the five were the first three,
then the fifth and sixth. `startup_readiness` and the three
cost-ledger tables were ungranted, which is how a deployed app sat at `/readyz`
503 with the app role holding `arw` on `ring_lease` and no ACL entry at all on
the other four. Run the block by hand only against an installation provisioned
before that fix; a fresh install gets the whole set from setup.

`ensure_coordination` creates every object in the block for the same reason.
A `GRANT` names a relation that must already exist, and setup grants one
provision before the operator's first local serve — so leaving
`startup_readiness` and the cost tables to be created by whichever process
first runs with `CREATE` meant the grants could not have been issued even if
they had been listed.

Round 4's separate production Postgres is unaffected and keeps its own
`CONNECT` / `USAGE` / `SELECT`.

---

## Required patch to `server/lifecycle.py`

**This has landed.** It is kept here because it is the reference for what the
function is supposed to do, and because bootstrap's third refusal is a text-level
grep against it: `bootstrap.sh` looks for `terraform-backend.json` in
`server/lifecycle.py`, and `tests/test_lifecycle.py` pins that string so renaming
`BACKEND_RECORD_NAME` cannot silently reopen the refusal.

Without it, `_terraform_init` hardcodes `-backend-config=path=<state>`, which is
a local-backend argument; the S3 backend rejects an unknown argument outright.

`_terraform_init` (now around `server/lifecycle.py:396`, and covered by five
tests in `tests/test_lifecycle.py` that run no Terraform command) reads:

```python
BACKEND_RECORD_NAME = "terraform-backend.json"
BACKEND_OVERRIDE = AWS_INFRA_DIR / "backend_override.tf"


def _backend_record(manifest: DemoManifest) -> dict[str, Any] | None:
    """Read this generation's opt-in remote backend, if it has one.

    Absent -- the default and the only state any existing installation has --
    this returns None and everything below behaves exactly as it always has.
    """
    record = manifest_path().parent / BACKEND_RECORD_NAME
    if not record.is_file():
        return None
    payload = json.loads(record.read_text(encoding="utf-8"))
    if payload.get("backend") != "s3":
        raise RuntimeError(f"{record} names an unsupported backend {payload.get('backend')!r}")
    for key in ("bucket", "key", "region"):
        if not payload.get(key):
            raise RuntimeError(f"{record} is missing {key!r}")
    return payload


def _terraform_init(manifest: DemoManifest) -> None:
    record = _backend_record(manifest)
    if record is None:
        # A stale override from another generation would silently point this
        # init at that generation's state, so it is removed, not left.
        BACKEND_OVERRIDE.unlink(missing_ok=True)
        state = Path(manifest.aws.terraform_state)
        state.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        backend_arguments = [f"-backend-config=path={state}"]
    else:
        # Terraform cannot interpolate a backend block, so the values are
        # written as literals and regenerated on every init.
        BACKEND_OVERRIDE.write_text(
            'terraform {\n'
            '  backend "s3" {\n'
            f'    bucket       = "{record["bucket"]}"\n'
            f'    key          = "{record["key"]}"\n'
            f'    region       = "{record["region"]}"\n'
            '    use_lockfile = true\n'
            '    encrypt      = true\n'
            '  }\n'
            '}\n',
            encoding="utf-8",
        )
        backend_arguments = [
            f"-backend-config=bucket={record['bucket']}",
            f"-backend-config=key={record['key']}",
            f"-backend-config=region={record['region']}",
        ]
    _run(
        _terraform_base() + ["init", "-input=false", "-reconfigure", *backend_arguments],
        env=_terraform_environment(manifest),
    )
```

`AWS_INFRA_DIR` and `json` are already imported in that module. `-reconfigure`
is already there and is what makes switching between two generations with
different backends safe: it discards the previous backend's cached
configuration rather than offering to migrate.

Two properties worth preserving in review:

- **A generation with no record behaves byte-identically to today.** No existing
  installation changes.
- **The override is regenerated or deleted on every init**, so the shared
  `infra/aws/` working directory cannot leak one generation's backend into
  another's.

---

## Change record: edits this feature needed in files it could not touch

Historical, and of no use when installing — kept as the audit trail for how the
S3 backend and the automated deploy landed across files owned by other changes.
Every row is done.

| File | Change | State |
|---|---|---|
| `docs/iam/` | Adopt the four-statement S3 state draft as `anti-demo-operator-4-state.json`. The draft itself was never committed, so it is named here for history only and is not a path in this tree. | done, with one correction: `DeleteObject` split into its own statement so the policy matches its own rationale. Five statements, 1059 characters; merging into the largest existing file would reach 5249 of 6144. |
| `docs/iam/README.md` | Document the fourth file: needed only for `--state-backend s3`; note the three flagged items above (`DeleteObject` scoped to `.tflock`, prefix-conditioned `ListBucket`, unscopeable `ListAllMyBuckets`). | done, and it leads with the fact that the backend cannot run yet |
| `docs/BOOTSTRAP.md` | The "Deploying as a Databricks App — not covered" section is now wrong. It is covered; link here. The "Terraform state is local, and stays local" item (now under "What needs more than the six inputs") needs "unless you opt into S3; see docs/DEPLOY.md". | done |
| `docs/bootstrap.env.example` | Add `ANTI_DEMO_TF_BACKEND`, `ANTI_DEMO_TF_STATE_BUCKET`, `ANTI_DEMO_TF_STATE_KEY`, `ANTI_DEMO_SECRET_SCOPE`, all commented out. | done |
| `app.yaml` | **No change needed.** Its comment on `AWS_SESSION_TOKEN` documents the same conclusion this deploy implements: Databricks Apps has no optional binding, so the resource must exist even for a credential that carries no token, and in that case the correct value is the empty string — which botocore (`if token:`) and `server/aws_auth.py` (`bool()`) both read as absent, where a space would be truthy and get signed into every request. The deploy writes a real token when it has one and empty when it does not, and says which. | unchanged, deliberately |
| `README.md` | Point the app-deployment paragraph at `docs/DEPLOY.md`, and mention `--deploy-only` beside `antidemo setup`. | done |
| `CONTRIBUTING.md` | Note that `./bootstrap.sh` with no flags is the way to check whether the deployed app's seal is current. | done |

---

## What is untested

Stated plainly, because the from-scratch validation run is where most of these
get exercised for the first time.

**Not run at all while these two features were being written.** No Terraform
command in any form — not `init`, `plan`, `apply`, `validate` or even `fmt` — and
no `antidemo setup`. Everything below was verified by
`tests/bootstrap_stub_harness.sh` (73 assertions across 14 cases, run under
pytest by `tests/test_bootstrap_stub_harness.py`), which stubs `aws`,
`databricks` and `terraform` with their real response shapes, plus read-only
calls against a live workspace to confirm command and payload shapes. **The
deploy has since been run for real**, and what that settled and what it left
open is two paragraphs down. The S3 backend has not.

**The S3 backend has never been initialised.** The patch that makes it reachable
has landed and is unit-tested, but no `terraform init` has been run against it.
What is verified: the exact `init` argv in both modes, that the local mode is
byte-identical to its previous behaviour, that a stale override from another
generation is deleted rather than inherited, that an unsupported or incomplete
record refuses instead of falling back to local, the generated HCL character for
character, `use_lockfile` being correct for 1.11+, the bucket creation,
versioning, encryption and public-access-block CLI shapes, and that every guard
fires in the right order. What is not verified: that `terraform init` accepts the
generated override plus partial config on the first real run.

**The app deploy has been executed against a live workspace, and the app
served.** The two things a stub could never settle are settled: `apps update
--json @/dev/stdin` accepts this payload against a live workspace, and the tree
`databricks sync` produces is one the app can actually run from. The deployment
reached SUCCEEDED, `/api/health` answered `status ok` with its database
connections sealed, `/readyz` answered 200 repeatedly over several minutes, and
the app inventoried the account itself and reported `installation_state
verified_present` with all twelve sealed resources there and its own AWS
principal named.

Worth knowing how that was established, because two of the obvious sources
mislead. The success was read from the workspace's deployment **history**, not
from `app-deploy.json` — that file records only the last *success*, so a run of
later failures behind it is invisible; the deployment immediately before this one
is `CANCELLED`, by the installer's own stop/start, rather than failed. And that
it is genuinely the deployed app and not a laptop was established from the app's
own log stream: a traceback rooted under `/app/python/source_code/server/`, on
the managed Linux CPython uv fetched into `/app/.uv-python/`, authenticating as
the app's service principal with `auth_type=oauth-m2m`. The maintainer's laptop
is macOS on a Homebrew interpreter under a named human identity, so no local
process could have written that line.

Still verified only against stubs: every failure branch — scope-exists,
scope-create-races, secret-write-failure, resource-bind-failure, sync-failure,
deploy-failure and app-never-becomes-ACTIVE. A run that succeeds exercises none
of them.

**Two bouts have since been driven from the deployed app.** Rounds 4 and 6 were
first refused at arm by Databricks-side grants the app's service principal did
not hold — a defect in what setup grants, since fixed. With those grants issued,
Round 4 verified in 8650.27 ms, reading the scored row back through the app at
its sealed score and model version, and Round 6 verified in 12387.95 ms, with a
checkout committed in 185.88 ms, a guardrail row committed in 205.46 ms and read
back out of Delta in 195.06 ms inside that total. Both figures are Lakebase-lane
elapsed times. An earlier Round 6 bout also reached verified and is deliberately
not the one quoted, because its cleanup was still failing at the time.

Deployed origin was established the same way the deploy was, from the app's own
`/logz` stream rather than inferred: tracebacks rooted at
`/app/python/source_code/server/live_orders.py`, again on the managed Linux
CPython under `/app/.uv-python/` and authenticating as the app's service
principal with `auth_type=oauth-m2m`, where the laptop's interpreter lives under
`/opt/homebrew/` and runs as a human user. Worth naming the asymmetry with the
Aurora results in `README.md`, since it is easy to read these as the same grade
of evidence: those were corroborated by a launch record on disk, and these rest
on the container's own log lines. Both are sound. They are not the same kind of
proof.

**Neither bout is a race, and the payload will tell you otherwise if you let
it.** Both competitor lanes are `not_supported`, with `attempts: 0` and
`elapsed_ms: null`, reading "AWS lane not timed for this Managed Sync proof" and
"AWS CDC pipeline not built or timed"; `comparison` is `kind: capability_gap`
with `margin: null`. Those are structural disclosures, not measurements —
nothing Aurora ran, in either bout. The trap is live in the payload and has
caught a reader once already: `competitor.id` is `aurora_serverless_v2` and the
lane renders as "Aurora Serverless v2" regardless, so anyone reading the id
without the state manufactures a race that never happened. `launch_skew_ms` is
`null` on both bouts, with `same_client`, `same_transaction` and `same_nonce` all
true: skew is populated only when two lanes actually launch, so that null is an
absence and must not be presented as a zero.

**What those two bouts do not extend to.** They were the only two rounds *those
bouts* proved, and the reason is worth keeping even though the limit itself has
since been lifted. At the time, Rounds 1, 2, 3 and 5 needed AWS and were refused
from a deployed app, which is both why 4 and 6 were the reachable pair and why
the credential expiry never gated them: a credential fault stops only the
AWS-backed rounds, and those were already refused for reasons no credential
settles. Rounds 1, 2 and 3 race an opponent over TCP 5432 behind a security group
that then admitted one operator CIDR, while a deployed app egresses from
Databricks-managed addresses rather than the operator's; Round 5 is withheld from
a deployed app by `server/round_availability.py` on any installation that sealed
no shared runtime role. That gate does not ask which principal the app
authenticated as, so it holds whether or not the published credential happens to
be the one the control role trusts.

> **Superseded, 2026-08-25.** Both refusals are conditional rather than
> structural, and an installation has since cleared both. Sealing the four
> Databricks-published serverless egress prefixes into the manifest and applying
> them to the database security groups opens the network path, and sealing a
> shared runtime role at first provision opens Round 5's — see
> [Round 5 from the deployed app](#round-5-from-the-deployed-app-the-two-hop-assume).
> All six rounds have now been declared from the deployed app with sealed
> receipts. A **default** install still refuses all four, and neither switch is a
> repair: both are first-provision decisions. The paragraph above is kept because
> it explains the two bouts below, not because it still describes the ceiling. The temporary session the app is bound to is a separate
unfinished question —
[How the deployed app gets AWS credentials](#how-the-deployed-app-gets-aws-credentials-and-what-breaks-them)
— and settling it would not make any of those four reachable. Neither bout left
a receipt: `GET /api/receipts` on the deployed app answered `{"receipts": []}` and
the log carried "No artifact root is selected, so there is nowhere to keep a bout
receipt". That was never a structural gap — `DurableReceiptStore` writes to
`anti_demo_coordination.bout_receipt`, `app.py` installs it at startup, and the
grant for it is in the block above — it was a runtime that predated all three.
**The redeploy that closes it has happened**, at `2026-08-24T04:39:11Z`, and the
receipt table has carried rows sealed after it ever since; the next section is
what those rows are and how their origin was separated. What a redeploy cannot do
is retro-fit a receipt onto the two bouts quoted above. Those two stay
log-derived, and this section and [`NOTICE.md`](../NOTICE.md) are where that is
recorded: `README.md` quotes neither figure, so it carries no marker for them, and
its evidence table flags one earlier log-derived Round 6 run without distinguishing
the Round 4 one. Treat the deploy as proven, those two rounds as proven from it,
those two figures as log-derived, and the deployed runtime as no longer
receipt-less.

### Deployed origin is deduced, not stamped

**No receipt field records where a bout ran.** `BoutReceipt` in `server/receipts.py`
has no origin attribute, so nothing in a sealed row says "container" or "laptop".
Every "from the deployed Databricks App" in this document and in `README.md` is an
inference from *where the receipt landed*: a locally run server writes a file under
the generation's `receipts/` directory as well as the coordination row, while the
deployed container has no artifact root at all and can only leave the row. A row
with no matching file therefore places the bout outside this filesystem. The
inference holds, and it is checkable by hand, though no longer against
`README.md`: that file now quotes a single receipt code, `EECDD4D6`, and that one
does have a file. Check it against the receipt tree instead. It currently holds 59
files, every one of them under generation `v7` or `v8`, so any code in
`anti_demo_coordination.bout_receipt` with no file among them names a bout this
filesystem never ran. But it is still an inference, and its strength is not
uniform — the section below is one set of six where it was pushed on with
fingerprints inside the receipts, a local access log and a latency split, while the
rest stand on the row-without-file test alone. Adding a field to `BoutReceipt` that
stamps the runtime at seal time, from the same `ANTI_DEMO_ENV` /
`DATABRICKS_APP_NAME` signal `safe_change_live.py` already branches on, would make
origin a recorded fact and retire this paragraph. Until someone does, read deployed
origin as the strongest available deduction rather than as something the receipt
says.

**This does not reach the measurements.** Elapsed times, margins, lane verdicts and
censoring flags are read out of sealed receipts and are unaffected by any of the
above. It is only the *origin* attribution that is deduced.

### Receipts that exist only in the coordination table

Six receipts are in `anti_demo_coordination.bout_receipt` with no matching file
under the generation directory: three failed schema-change bouts, one cancelled
and one failed wake bout, and one Round 6 bout that verified in 11986.30 ms. All
six sealed between `04:46:12Z` and `04:55:56Z` on 2026-08-24, after the
`04:39:11Z` redeploy and after the last receipt that did land on disk.

**Durable-only is the deployed shape, not a broken local one**, and the receipts
say so themselves rather than by elimination. The three failed schema-change
bouts each carry a Postgres refusal naming the **app's** service principal client
ID as the connecting user. `safe_change_live.py` picks that user in exactly one
branch — `app_mode`, meaning `ANTI_DEMO_ENV=databricks-app` or `DATABRICKS_APP_NAME`
set — and a locally run server takes the other branch and connects as
`manifest.databricks.user`. The failed wake bout carries "Missing explicit
Lakebase target binding: DATABRICKS_PROFILE", which a local server cannot
produce, because `manifest.apply_manifest_environment` always exports that
variable and `app.yaml` deliberately never sets it. Four of the six therefore
carry a positive deployed fingerprint inside the receipt.

The remaining two — the cancelled wake and the verified Round 6 — carry no such
text, and were placed by exclusion plus one corroborating measurement. The local
server's own access log covers that whole window and records no session POST in
it; every bout it did run in the surrounding hours has a file on disk. And the
gap between `sealed_at` and the table's `written_at` is cleanly bimodal: about
0.7–1.0 s for every bout that also landed on disk, and 0.05–0.08 s for all six
that did not — a laptop reaching the coordination endpoint over the internet
against a writer already inside the workspace network.

**`/logz` could not be used to close it this time.** The websocket handshake
refuses a service-principal bearer token with HTTP 403, exactly as it did before
a browser session was used. So the two quiet receipts rest on exclusion and on
that latency split, not on the container's own log lines like the two bouts
above. Worth naming: it is a grade below the evidence for the 8650.27 ms and
12387.95 ms figures, which is why nothing in `README.md` quotes it.

**Nothing here implies the local receipt directory has stopped working**, and
that was checked rather than argued. A Round 4 bout run on the local server
hours after the six rows above verified in 8631.95 ms and left its file in the
generation's `receipts/` directory, alongside the arm that had expired minutes
before it — so disk writes are landing now, not merely believed to be. The
mechanism agrees: `process_registry.register_serving_process` writes the running
server's launch record only when `state_dir_from_environ()` resolves to a real
directory, which is the same call `receipts.artifact_root()` makes, so a server
with nowhere to write could not have left the record that says where it writes.

The failure mode is worth naming anyway, because it is quieter than it looks. On
a process that has a durable store, `record_sealed_bout` treats a file write with
nowhere to go as the ordinary deployed shape and logs it at **debug**, not
warning — the warning is reserved for the case where neither store is available.
So a local server that genuinely lost its artifact root would keep sealing
receipts into the coordination table and say nothing at default log level. The
merged `/api/receipts` view would look complete, and only the absence of files
would show it. That is a real gap in the signal, not a defect found: nothing
observed here has hit it.

**Drift detection is verified in the direction that matters and not the other.**
A changed manifest is detected reliably. A *matching* seal cannot be proven,
because the secret value is unreadable. The design fails toward "republish".

**The apply tail is structurally unreachable from a test.** It ends in
`./antidemo setup`, which is invoked by relative path and so cannot be intercepted
by a PATH stub, and which provisions real infrastructure. The two artefacts it
writes — the backend record and the override HCL — are checked directly instead
(`case_generated_artefacts`). The bucket-creation calls themselves are covered
only as far as the CLI argument shapes.

**The harness touched no installation.** No file in `.anti-demo-v7/` — then the
live generation, since superseded by `v8` — was written by it, its state file and
manifest were fingerprinted before and after and are byte-identical, no
`backend_override.tf` was generated into `infra/aws/`, and the
only change to `infra/aws/versions.tf` is comments — the functional content,
`backend "local" {}` included, is identical. The S3 path refuses that generation
outright, by design, at the first of four guards, and
`case_no_regression` asserts the fingerprint as part of every harness run.
