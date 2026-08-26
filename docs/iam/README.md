# AWS IAM policies for the Anti-Demo operator principal

These policies are the minimum permission set for the AWS principal whose
access keys drive `bootstrap.sh`, `antidemo setup`, and `antidemo doctor`.
They were derived by reading `infra/aws/*.tf` and the `boto3` calls in `server/`
— not from an AWS reference list — so every action below is traceable to a
declared resource or an actual API call.

Files 1–3 are required. File 4 is only for the opt-in S3 state backend — see
[the fourth file](#4--terraform-state-in-s3-opt-in) before you attach it.

**The deployed Databricks App does not use this principal.** It authenticates as
a separate, much narrower IAM user — see
[the app's own principal](#the-deployed-apps-own-principal) — which can run the
rounds and clean up after them and can do nothing else. An earlier version of
this page said the operator principal drove the app as well; it does not, and
attaching the operator set to the app would hand a public-facing process the
ability to create IAM roles and launch EC2 instances.

## Why several files and not one

A customer-managed IAM policy is capped at 6144 characters of non-whitespace
JSON. The full permission set does not fit. The split is by service boundary so
a cloud team can review and approve each one independently:

| File | Covers | Packed size | Required |
|---|---|---|---|
| `anti-demo-operator-1-network.json` | EC2 discovery, security groups, the Round 5 runner instance, SSM | 2162 | yes |
| `anti-demo-operator-2-databases.json` | RDS: Aurora Serverless v2, RDS PostgreSQL, subnet groups, RDS Proxy | 2356 | yes |
| `anti-demo-operator-3-identity.json` | IAM, Secrets Manager, KMS grants, STS, CloudWatch reads | 4228 | yes |
| `anti-demo-operator-4-state.json` | S3: the Terraform state bucket, the state object and its lock | 1059 | only with `--state-backend s3` |

Every one of the four is comfortably inside the limit; the largest, file 3, has
1916 characters of headroom. Attach 1, 2 and 3 — any one alone is not
sufficient.

File 4 is a separate file rather than merged into one of the other three because
the default install needs none of it: keeping it separate means a local-backend
install grants nothing for S3 at all. Merging is safe on size — folding it into
file 3, the largest, reaches 5249 of 6144 — but it is not the shape to prefer.

## Rendering and attaching

Files 1–3 use `<AWS_ACCOUNT_ID>` and `<AWS_REGION>` placeholders. Replace both
before attaching:

```bash
ACCOUNT=111122223333
REGION=us-west-2
for f in docs/iam/anti-demo-operator-[123]-*.json; do
  sed -e "s/<AWS_ACCOUNT_ID>/$ACCOUNT/g" -e "s/<AWS_REGION>/$REGION/g" "$f" \
    > "/tmp/$(basename "$f")"
done

# Then, as an account administrator:
aws iam create-policy --policy-name AntiDemoOperatorNetwork \
  --policy-document file:///tmp/anti-demo-operator-1-network.json
aws iam create-policy --policy-name AntiDemoOperatorDatabases \
  --policy-document file:///tmp/anti-demo-operator-2-databases.json
aws iam create-policy --policy-name AntiDemoOperatorIdentity \
  --policy-document file:///tmp/anti-demo-operator-3-identity.json
```

File 4 carries a third placeholder, `<STATE_BUCKET>`, which is why the loop
above skips it. Only render and attach it if you are opting into S3 state:

```bash
STATE_BUCKET=my-tfstate-bucket
sed -e "s/<STATE_BUCKET>/$STATE_BUCKET/g" \
  docs/iam/anti-demo-operator-4-state.json > /tmp/anti-demo-operator-4-state.json
aws iam create-policy --policy-name AntiDemoOperatorState \
  --policy-document file:///tmp/anti-demo-operator-4-state.json
```

`bootstrap.sh` verifies the resulting permissions before spending anything: it
runs every read that Terraform and the app need, plus real EC2 authorisation
dry runs for `CreateSecurityGroup` and `RunInstances`.

## The deployed app's own principal

`anti-demo-app-runtime.json` is a fifth document, and it is not part of the
operator set. It is what the app runtime IAM user holds: enough to arm and run
Rounds 1, 2, 3 and 5, and to delete what they create, and nothing more. It packs
to 2933 characters, so it needs no split.

| Grants | Withholds |
|---|---|
| RDS catalog reads across the region | every write outside the per-bout `adsc-*` and `adrc-*` prefixes |
| PITR restores and the instances they need into those prefixes | any change to the sealed Aurora and RDS residents themselves |
| deletes, so a finished bout stops costing money | `iam:*`, `ec2:RunInstances`, `ssm:*` |
| `sts:AssumeRole` on `role/*-r5-exec-*` and nothing else | assuming any other role in the account |
| `secretsmanager:GetSecretValue` on `secret:rds!*` only | every other secret in the account |
| KMS use gated on `kms:ViaService` for RDS and Secrets Manager | KMS used directly, for anything |

Two consequences worth stating rather than discovering:

- **The `sts:AssumeRole` grant is what puts Round 5 within this principal's
  reach, and it is narrow on purpose.** Round 5's live lane assumes a control
  role and does its work on the returned session, so without this statement the
  round is refused before it starts. The resource pattern admits only the Round 5
  execution role — `role/*-r5-exec-*` — and no other role in the account, which
  matters because the control role itself can create IAM roles and launch
  instances. Reaching the role is still not sufficient: the control role's own
  trust policy names exactly one principal, and this user is only Round 5's
  principal on an installation sealed to it.
- **Deletes are the load-bearing half.** A round that measures correctly and
  then cannot delete its artifact leaks a running database. The delete
  statements are scoped to the two per-bout prefixes, which is what makes
  granting them safe.

Render and attach it the same way as the others:

```bash
sed -e "s/<AWS_ACCOUNT_ID>/$ACCOUNT/g" -e "s/<AWS_REGION>/$REGION/g" \
  docs/iam/anti-demo-app-runtime.json > /tmp/anti-demo-app-runtime.json
aws iam create-policy --policy-name AntiDemoAppRuntime \
  --policy-document file:///tmp/anti-demo-app-runtime.json
```

`tests/test_aws_permission_coverage.py` holds this file to what the code
actually calls. The required set is recovered from the round modules' own call
sites by `server/aws_permissions.py` rather than listed in the test, so adding a
`boto3` call to a round fails the suite until the policy catches up. Run
`python -m server.aws_permissions` to print the current plan.

## What each statement is for

### 1 — network and compute

- **`DiscoverNetworkAndFleet`** — `infra/aws/network.tf` reads the default VPC,
  its default-for-AZ subnets, and the runner subnet's route table (it asserts a
  direct `0.0.0.0/0` route to an Internet Gateway, `round5_runner.tf:186`). The
  AWS provider also refreshes instance, volume and security-group state on every
  plan.
- **`ManageDemoSecurityGroups`** — 8 security groups and 1 egress rule:
  4 Aurora, 3 RDS, 1 runner (`network.tf`). Round 5 creates one more per bout at
  runtime (`server/connection_spike_live.py`). `vpc/*` is included because
  `ec2:CreateSecurityGroup` authorises the target VPC as a dependent resource.
- **`RunAndRetireRound5Runner`** — one `m6i.large` with a 20 GiB encrypted gp3
  root volume, IMDSv2 required (`round5_runner.tf:147`).
- **`ReadAmazonLinuxAmiPointerOnly`** — scoped to the single SSM public
  parameter path the AMI is resolved from (`round5_runner.tf:3`). This grants no
  access to any parameter you own.
- **`DriveRound5RunnerOverSsm`** — Round 5 executes its harness through
  `AWS-RunShellScript`; the control role in `round5_control.tf:27` is the
  narrowly scoped version of this, and the operator needs it directly for setup
  and for `antidemo doctor`.

### 2 — databases

- Resource ARNs are pinned to the identifier prefixes the code actually
  generates: `lakebase-ant*` for everything Terraform names
  (`locals.tf:v7_round_resource_names` truncates `name_prefix` to 12 chars),
  `adsc-*` for Round 2 isolated environments
  (`server/safe_change.py:deterministic_artifact_id`), and `adrc-*` for Round 3
  recovery environments (`server/recovery.py:deterministic_recovery_artifact_id`).
- **`rds:DescribeDBInstanceAutomatedBackups` is required, not incidental, and it
  is the one action here a least-privilege review would trim.** Round 3 restores
  a database to a point in time, so it has to know the window it may pick a
  point inside. `EarliestRestorableTime` is a member of the `DBCluster` shape and
  **not** of `DBInstance` — the RDS service model rejects it there — so the
  Aurora lane reads both bounds out of one `DescribeDBClusters` response and the
  RDS lane cannot. `RdsRecoveryAdapter._restorable_window`
  (`server/recovery_live.py`) therefore makes a second call and reads
  `RestoreWindow`, which lives only on the automated-backup shape. A policy
  assembled from "the lane describes instances and restores them" omits this and
  looks complete. The failure it buys is `AWS restorable window is unavailable`,
  raised mid-bout, naming the window and never IAM — so nothing about the
  symptom points at the policy. `tests/test_round3_iam_dependency.py` pins all
  three halves of this against the real RDS service model: the shape asymmetry,
  the operation the window comes from, and any policy that grants
  `rds:RestoreDBInstanceToPointInTime` without this read.
- **`ManageRound5ProxiesInThisRegionOnly`** is the one RDS statement on `*`.
  RDS Proxy names are per-bout and not known in advance, and `rds:CreateDBProxy`
  is evaluated against a resource that does not exist yet. It is constrained to
  the region only. The role the *app* assumes at runtime
  (`round5_control.tf:370`) constrains the same actions further with mandatory
  `aws:RequestTag`/`aws:ResourceTag` ownership conditions; this policy is the
  install-time superset.

### 3 — identity, secrets, keys

- **`ManageRound5Roles`, `...BoundaryPolicy`, `...InstanceProfile`** — the 3
  roles, 1 customer-managed permissions boundary and 1 instance profile in
  `round5_runner.tf`, `round5_secrets.tf` and `round5_control.tf`. Scoped by the
  `name_prefix` values in `locals.tf:round5_iam_stem`: `i<20 hex>-r5-*` for a v7
  installation and `r5-*` for the legacy layout. The `anti-demo-runtime*` ARN on
  the same statement is [the runtime role](#the-runtime-role-and-the-fortnightly-sweep);
  `iam:UpdateAssumeRolePolicy` on it is what lets `antidemo renew` repair a trust
  relationship the sweep broke.
- **`ManageTheSealedRuntimeRolePolicies`** — the four customer-managed policies
  attached to the runtime role, named `anti-demo-runtime-<n>-*`. They are
  separate from the role statement above because they are policies, not roles,
  and IAM will not let one ARN pattern cover both.
- **`PassOnlyDemoOwnedRolesToTheirOwnServices`** — `RunInstances` passes the
  runner role; RDS Proxy creation passes the proxy service role. Both are
  restricted by `iam:PassedToService`.
- **`ManageDemoOwnedSecrets`** — the 2 Terraform-declared secrets
  (`round5_secrets.tf`) plus the `rds!*` secrets that RDS itself creates because
  every database sets `manage_master_user_password = true`.
- **`UseAwsManagedKeysForRdsAndSecretsOnly`** — `storage_encrypted = true` on
  every database and volume, and RDS-managed master passwords, both need KMS.
  Every action is gated on `kms:ViaService`, so the principal cannot use any KMS
  key directly.

### 4 — Terraform state in S3 (opt-in)

**Attach this only if you are opting in.** `bootstrap.sh` no longer refuses
`--state-backend s3` — the `_terraform_init` patch it required has landed — but
the backend is opt-in and new-installations-only, so on a default install this
policy grants S3 access that nothing will use. The four refusals that precede it,
and what remains unverified, are in [`docs/DEPLOY.md`](../DEPLOY.md).

The default install keeps Terraform state in the generation directory next to
the manifest and needs none of this.

Three things a reviewing cloud team asks about, in the order they ask:

- **`s3:DeleteObject` is scoped to `*.terraform.tfstate.tflock`, and not to the
  state object.** Terraform never deletes state — `destroy` writes a state with
  no resources in it — but it does delete its own lock on unlock. Delete on the
  state object would let a mistake or a bad actor erase the only record of what
  you own, so `DeleteOnlyTheLock` is its own statement with its own single ARN
  rather than a third action on the read/write statement. That separation is the
  one change made to this policy while adopting it: as drafted, `DeleteObject`
  sat alongside `GetObject` and `PutObject` on a two-ARN resource list, which
  grants delete on the state object as well — the opposite of what the rationale
  above describes. Splitting it costs two statements and 132 characters.
- **`s3:ListBucket` is conditioned on the `anti-demo/*` prefix.** Bucket-level
  list is what enumerates objects, so without the condition this principal could
  inventory an unrelated bucket it happened to be pointed at. With it, the
  listing stops at the demo's own prefix.
- **`s3:ListAllMyBuckets` is account-wide, and cannot be scoped.** That is the
  API's shape, not a choice here: it takes no resource. It reveals bucket *names*
  only — no contents, no policies, no tags — and one preflight probe uses it to
  tell "the bucket does not exist" apart from "you cannot see it". Drop the
  `DiscoverBucketsForThePreflightProbe` statement if your team objects; the probe
  then reports as denied and everything else still works.

If your cloud team would rather pre-create the bucket than let bootstrap create
it, drop `CreateAndHardenTheStateBucket` as well and create the bucket with
versioning enabled, SSE-S3 default encryption and all four public-access blocks
set. Bootstrap detects an existing bucket, reports its region, and warns if
versioning is off.

## The runtime role, and the fortnightly sweep

Everything above describes permissions attached to a *human's* IAM user. From the
next fresh install there is a second thing to know about: a role called
`anti-demo-runtime`, declared in `infra/aws/anti_demo_runtime.tf`, which carries
these same three policies and is assumed by everyone.

### Why it exists

The deployed Databricks App authenticates as an IAM user, because
`server/aws_auth.py:validate_app_aws_environment` hard-requires static keys and
no Databricks-native keyless path is bindable to an app runtime today. A laptop
can authenticate as either — as the app runtime user, by letting the launcher
read `.env.bootstrap`, or as an Identity Center permission-set role, by exporting
SSO session credentials. Those are two different principal ARNs, and
`Round5Resources.control_role_trusted_principal_arn` seals **exactly one**, so
whichever is sealed, the other loses Round 5.

That is a real cost either way round, and it is the reason to want this role. An
installation sealed to the app runtime user gets Round 5 from the deployed app
and from a launcher-credentialled laptop, and loses it from an SSO shell — which
is awkward, because `antidemo doctor` needs that same SSO shell for its EC2 reads.
An installation sealed to the SSO role has the mirror-image problem.

A single role trusted by both collapses that. Whoever starts the process,
`sts:GetCallerIdentity` returns `assumed-role/anti-demo-runtime/<session>`, and
`principal_matches` resolves it to the same sealed ARN either way. The Round 5
control role then trusts the runtime role — still exactly one principal, which is
why the two-principal change does not disturb `round5_secret_free_topology`.

The role, its three policies and its three attachments are all free. This adds
**$0** to the bill.

### What it asks the operator for

Nothing new on a default install. `ANTI_DEMO_RUNTIME_PRINCIPAL_ARNS` — a
comma-separated list of the exact IAM user and role ARNs to trust — is read at
first provision only. Leave it unset and no role is created and nothing changes.
Set it once and the ARNs are **sealed** into the manifest from Terraform's own
output, after which the environment variable is only allowed to agree: contradict
the seal and `antidemo setup`, `antidemo doctor` and `antidemo renew` all refuse, exactly as
they do for the Round 4 catalog. Changing who may assume the role needs a fresh
install, not an environment variable.

### The trap in it, which is the reason `doctor` and `renew` both grew a check

**A trust policy that names an IAM user stores that user's unique principal ID,
not its ARN.** AWS documents this for IAM users verbatim. Delete the user and
recreate it with the same name — which is exactly what a sweep and a re-install
do — and the trust relationship is permanently broken, while every name in sight
still matches.

That failure mode is worse than it sounds, because `principal_matches` compares
by name. A recreated user passes the credential probe, `/readyz` stays green, the
catalog offers Round 5, and the `AssumeRole` is denied **after the bell**, in
front of an audience.

`_anti_demo_runtime_trust_check` in `server/lifecycle.py` is the only thing that
can see it. It does not compare a name to a seal; it reads the live trust
document with `iam:GetRole` and believes only that. Three things fail it:

- a bare unique principal ID in `Principal` — IAM reverse-maps stored IDs back to
  ARNs only while the principal exists, so seeing one *is* the deletion;
- a sealed principal missing from the live document, with `iam:GetUser` /
  `iam:GetRole` consulted to say whether it is gone from IAM entirely;
- a principal in the live document that this installation never sealed — someone
  widened who can assume it, outside Terraform.

The check runs immediately after `aws_identity` in `antidemo doctor`, and it is
advisory-and-silent on any installation that seals no runtime role.

### The second trap, which `renew` itself used to spring

**Round 5's per-bout ownership tags are not labels. They are half of a
credential.** `round5_control.tf` grants `ec2:CreateTags` on
`security-group-rule/*` only when the request carries `expires-at` — along with
the run id, the slug and both spellings of owner — *equal to* the values
Terraform was given. So the sealed copy of that tag set and the applied policy
have to agree exactly, and one of them can move without the other noticing.

`antidemo renew --ttl-hours N` moves the installation expiry and re-applies that
Terraform. A re-seal that carried the previous tag set forward therefore left the
manifest naming an expiry the policy had just stopped allowing. This is the same
shape as the trust-policy trap above and it fails the same ugly way: the bout
arms, the per-bout security group is created, and the third mutation dies two
seconds in with `UnauthorizedOperation … no identity-based policy allows the
ec2:CreateTags action` — journaled as the bare string `provider_create_failed`
and shown to the room as "The Round 5 setup phase failed". It cost a bout on
2026-08-24.

Both ends were fixed rather than one:

- **The tag set is one derived definition** (`_round5_ownership_tags` in
  `server/lifecycle.py`), rebuilt from the manifest on the first seal and on
  every re-seal, and validated against Terraform's own
  `round5_bout_base_tags` output before it is written. It is never carried
  forward from a previous seal.
- **`doctor` compares the seal against the live policy**, with one read-only
  `iam:GetRolePolicy`: it parses the `ec2:CreateTags` statement's condition and
  fails naming the drifted tag, instead of letting the bout discover it after
  the bell.

If you re-seal Round 5 by any route other than `_prepare_and_reseal_round5`, the
tags and the two Round 5 digests go out of step and the round stops arming.
There is no supported hand-edit.

### The sweep ran, bring it back

The AWS sandbox this was developed in is swept roughly every 14 days by its own
account automation, which deletes Aurora, RDS **and the IAM users**. Your account
probably does not do this, in which case you will never need this section. Where
it does happen it is a routine scheduled operation rather than an incident, and
the design target is "runs unattended *between* sweeps", not "survives one".

1. **Recreate the IAM user** with the same name, and issue new access keys.
   Attach files 1–3 to it as above.
2. **Update the app's credentials** — the `aws-access-key-id` and
   `aws-secret-access-key` app resources in `app.yaml`, then restart the app.
3. **`antidemo doctor`.** Expect `anti_demo_runtime_trust` to fail naming the bare
   unique ID. If it passes, the sweep did not take the user and there is nothing
   to repair.
4. **`antidemo renew`.** It reports what it is repairing before it plans, re-applies
   the sealed trust document — Terraform re-resolves the sealed ARNs to the new
   unique IDs — and then **re-reads the live policy to prove the repair took**.
   An apply that exits zero but leaves the trust unresolved is reported as a
   failure with the exact state it leaves behind, not as success. The seal is
   untouched throughout: the ARN strings never changed, only the unique IDs IAM
   stores behind them.
5. If the databases were swept too, this is a fresh install rather than a repair.
   `antidemo doctor` will say so first.

Only step 4 is new. Steps 1 and 2 are what the sweep has always cost.

## Flagged: the parts that are broader than ideal, and why

Give these to whoever approves the policy.

1. **`iam:CreateRole` / `iam:PutRolePolicy` / `iam:PutRolePermissionsBoundary`,
   scoped only by role-name prefix.** This is the most sensitive grant here.
   IAM has no tag-on-create condition strong enough to fence role creation, and
   Terraform uses `name_prefix` (a provider-generated suffix), so the exact name
   is not knowable in advance. A principal with this can create a role whose
   name starts with `r5-` and attach an inline policy to it. The permissions
   boundary in `round5_runner.tf:77` caps what the *runner* role can do, but it
   does not cap what this operator principal can create. **Mitigation:** attach
   an SCP or an IAM permissions boundary to the operator principal itself in a
   shared account, and prefer a dedicated sandbox account.
2. **`ec2:RunInstances` and `ec2:CreateVolume` on `*`, region-scoped only.**
   EC2 cannot restrict instance type or AMI through a resource ARN. The demo
   needs exactly one `m6i.large`. **Mitigation:** add a condition on
   `ec2:InstanceType` and `ec2:Vpc` if your account standard requires it; the
   demo will still work.
3. **`rds:CreateDBProxy` and `rds:CreateDBInstance`-class actions where the
   resource does not yet exist.** Create actions are authorised against a
   not-yet-existing ARN, so prefix scoping works for the names Terraform picks
   but not for per-bout Proxies. Region-scoped only.
4. **`secretsmanager:GetSecretValue` on `rds!*`.** This is every
   RDS-managed master password in the account, not only this demo's. RDS names
   those secrets itself and the demo cannot predict the suffix. **Mitigation:**
   in a shared account, replace `rds!*` with the exact secret ARNs after the
   first apply; `antidemo doctor` will tell you what they are.
5. **`kms:CreateGrant`.** Required for RDS to encrypt storage and for Secrets
   Manager to wrap master passwords. Gated on `kms:ViaService` for the three
   services involved, which is the tightest form AWS supports here.
6. **No `iam:SimulatePrincipalPolicy`.** Deliberately omitted:
   `bootstrap.sh` proves permissions with real read calls and EC2 dry runs
   instead, so the policy does not need to grant a policy-introspection API.

## What is *not* in these policies

- **`terraform destroy` / `antidemo cleanup --yes`** uses the same actions as apply
  (`Delete*`, `Terminate*`, `Revoke*`), all of which are present.
- **Anything the deployed Databricks App needs beyond these.** The app receives
  the same keys through the `aws-access-key-id` / `aws-secret-access-key` app
  resources in `app.yaml` and then calls `sts:AssumeRole` into the Round 5
  control role, which carries its own tightly scoped policy. The
  `AssumeTheRound5ControlRole` statement is what makes that hop legal — and, from
  the next fresh install, the hop into
  [the runtime role](#the-runtime-role-and-the-fortnightly-sweep) that precedes
  it.
- **Cost Explorer or the Pricing API.** `server/cost_model.py` uses static
  reviewed rates and Databricks system tables, not live AWS pricing calls.
- **Any S3 access at all, in a default install.** State lives on disk next to
  the manifest, so files 1–3 grant nothing for S3. File 4 is the whole of the S3
  surface and is opt-in.
- **DynamoDB.** The S3 backend uses S3-native locking (`use_lockfile = true`),
  so there is no lock table to create, pay for, or fail to destroy.
