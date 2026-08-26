# How the cost figures are produced

This page explains the method and the evidence behind the cost box. It states no
dollar figures of its own, on purpose: see [Why this page quotes no
totals](#why-this-page-quotes-no-totals).

- **What the installation costs:** [the cost box in
  README.md](../README.md#cost-and-safety).
- **The live rates and the sealed shape:** `server/cost_model.py`.
- **How the rates were checked against provider sources:**
  [PRICING_DISCOVERY.md](../PRICING_DISCOVERY.md), an archived audit dated
  2026-08-20.

## The two halves are not the same kind of number

This is the single most important thing on this page, and it is the opposite of
what most readers assume.

| Half | Source | Provenance | Has it met a bill? |
| --- | --- | --- | --- |
| Databricks | `system.billing.usage` quantities at `system.billing.list_prices` rates | `measured` | Yes, these are posted provider records |
| AWS | AWS Price List API rates multiplied by the sealed resource shape | `assumed` | **No. Never.** |

`ce:GetCostAndUsage` is denied to this installation and is not being pursued, so
no AWS figure anywhere in this repository has been reconciled against an invoice.
The AWS half is rate-card arithmetic. Its error bar runs upward: quantities that
are still pending are excluded rather than estimated, so a real bill can be
higher but is unlikely to be lower.

The receipts carry this structurally rather than only in prose. In the
2026-08-25 receipt every AWS component is tagged `provenance: assumed` and every
Databricks component is tagged `provenance: measured`.

## A per-day figure here is a rate, not a day's spend

Almost every figure in a receipt is built the same way, in
`server/standing_cost.py`: the amount accrued over the disclosure window is
divided by that window's hours and multiplied by 24. So a "per day" figure
answers *what would this cost over 24 hours at the rate observed in the window*,
which is not the same question as *what did this cost yesterday*. The exception is
the components declared intermittent, which are divided by their own uptime
instead; the rest of this section is why.

That distinction is invisible for a resource that genuinely exists all day, and
load-bearing for one that does not:

| Component | Exists for 24 h? | So its per-day figure is |
| --- | --- | --- |
| RDS instances, Aurora clusters, runner, addresses, secrets | Yes, from `--apply` until cleanup | what you actually pay per day |
| Round 4 synced-table pipeline | No. Started at arm, released 20 minutes after the bout settles | its rate while running, because this line alone is divided by uptime rather than by the window |
| App compute | Only while the app is in a `Running` state | the rate until someone stops the app |

The README states the standing figures and the conditional ones separately for
this reason. A table that lists all of them in one "cost per day" column reads
as though a pipeline that runs for minutes were billing around the clock.

**The pipeline row is where that method breaks, and neither README nor the app
uses it there any more.** `_hours()` in `server/posted_usage.py` measures the span
as `MAX(usage_end_time) - MIN(usage_start_time)` — the first posted interval to
the last — which for a component that never stops is also its uptime, and is why
that divisor is still the right one for the AWS lanes and for the App's own
compute. For a component that starts and stops it is not: every hour it was down
sits in the denominator. The receipt described below sampled the pipeline at a
62.5% duty cycle, 20.0 hours of uptime inside a 32.0-hour span, so its per-day
figure for that line is a duty-cycle-blended average of one installation's habits
rather than a rate while running. It is the lower of the two, so the error ran in
the unprotective direction for a warning.

The figure README publishes for that row is divided by uptime instead, measured
over 53.5 contiguous hours of it, which is why that one number does not reconcile
to this receipt and every other number here does. **The app's standing-cost panel
now divides that line by uptime as well, and agrees with the published rate.**
The pipeline is declared intermittent in
`standing_cost.INTERMITTENT_PLATFORM_COMPONENTS`, and only the components named
there change denominator; the query sums each meter's own posted intervals so the
uptime is read rather than assumed. The rate and the accrued amount stay separate
questions — the rate over the hours the pipeline was up, the amount over the
intervals actually posted — because collapsing them is what produced a panel that
contradicted its own documentation.

### What state each Databricks figure corresponds to

Databricks documents four app statuses, and only one of them bills. From the
[key concepts
page](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/key-concepts):
`Running` is "active and accessible" and "Databricks bills for the compute
resources used while the app is running"; `Stopped` "doesn't incur any costs";
`Deploying` and `Crashed` do not charge either. The
[overview](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/) adds
that apps "are billed per hour of compute time while running, based on
provisioned capacity", and
[compute sizing](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/compute-size)
prices that capacity per hour — `Medium` at 0.5 DBU/hour, `Large` at 1 DBU/hour.

Two consequences for reading this repository's App figure:

- **There is no traffic term.** A deployed app with nobody on it is `Running`,
  and `Running` bills its provisioned capacity. Apps publish no idle timeout and
  no scale-to-zero, so an idle app and a busy app of the same size cost the same.
  The App figure therefore applies from deployment until an explicit
  `databricks apps stop`, not just while someone is presenting.
- **Lakebase is the opposite case, and that is the demo's point.** Lakebase
  compute scales to zero, which is why the receipt's always-on minimum has
  nothing posted against it. Apps do not.

## Method

**The AWS half** is `count x published rate`, where the count comes from the
sealed installation shape and the rate from a public AWS offer file. Hourly items
are multiplied by 24 for a daily figure; monthly items are prorated over a
730-hour month. Nothing here is metered, which is exactly why it is `assumed`.

**The Databricks half** is posted usage. Rows are filtered to
`billing_origin_product = 'LAKEBASE'` and to this installation's exact Lakebase
project UID, joined to the rate active at each row's `usage_start_time`,
aggregated inside the price interval, and only then multiplied. Correction rows
stay in the sum. Compute uses the published `$0.26/DBU` promotional rate against
a `$0.52/DBU` normal list reference; storage stays at `$0.023/DSU`, which
publishes no promotion.

Two consequences worth stating:

- **A posted figure carries a watermark, not a total.** Provider ingestion lags
  by hours, so any Databricks figure is exact for the rows posted at query time
  and is never the completed daily invoice.
- **Nothing is labelled reconciled**, because with one half unmetered nothing
  can be.

## The 2026-08-25 receipt, sanitized

The cost box traces to one sealed receipt, `EECDD4D6`, generated
2026-08-25T02:09Z for region `us-west-2`. Receipts are not committed: they carry
live endpoints, project UIDs and run IDs. This is the publishable part of that
one, so the arithmetic can be checked by a stranger rather than taken on trust.

Six lanes, sixteen components. The AWS rows are fully reproducible: multiply the
quantity by the rate and you have that line. The Databricks rows carry posted
quantities, which move with every read, so they are named rather than fixed.

| Lane | Component | Cloud | Provenance | Quantity | Rate |
| --- | --- | --- | --- | ---: | --- |
| RDS | `db.t4g.medium` instances | AWS | assumed | 3 | `$0.065`/instance-hour |
| RDS | gp3 baseline storage | AWS | assumed | 3 x 20 GB | `$0.115`/GB-month |
| RDS | database public IPv4 | AWS | assumed | 3 of 7 | `$0.005`/address-hour |
| RDS | AWS-managed credentials | AWS | assumed | 3 of 9 | `$0.40`/secret-month |
| Aurora | Serverless v2 baseline compute | AWS | assumed | 4 x 0 ACU | `$0.12`/ACU-hour |
| Aurora | baseline storage | AWS | assumed | 4 x 1 GB | `$0.10`/GB-month |
| Aurora | database public IPv4 | AWS | assumed | 4 of 7 | `$0.005`/address-hour |
| Aurora | AWS-managed credentials | AWS | assumed | 4 of 9 | `$0.40`/secret-month |
| RDS Proxy | Terraform-managed proxy secrets | AWS | assumed | 2 of 9 | `$0.40`/secret-month |
| Neutral runner | `m6i.large` burst runner | AWS | assumed | 1 | `$0.096`/instance-hour |
| Neutral runner | gp3 root volume | AWS | assumed | 20 GB | `$0.08`/GB-month |
| Neutral runner | public IPv4 | AWS | assumed | 1 | `$0.005`/address-hour |
| Lakebase | always-on minimum compute | Databricks | measured | none posted | `$0.26`/DBU |
| Lakebase | database, PITR and snapshot storage | Databricks | measured | posted DSU/hour | `$0.023`/DSU |
| Databricks platform | Round 4 synced-table pipeline | Databricks | measured | posted DBU/hour | posted USD/DBU |
| Databricks platform | App compute | Databricks | measured | posted DBU/hour | posted USD/DBU |

Five details in that table do real work:

- **Aurora baseline compute is a structural zero, not a missing number.**
  `min_capacity = 0`, so an idle Aurora cluster parks for free. That is also
  what gives Round 1 something to wake up. The receipt renders it as zero with
  its reason attached rather than as `$0.00`.
- **The RDS Proxy lane is not zero even with no proxy running.** Two
  Terraform-managed secrets outlive every proxy.
- **Lakebase's always-on minimum has nothing posted against it**, because every
  sealed endpoint scales to zero. That is a measured absence rather than an
  assumption, which is why the lane still prices: its storage line does.
- **The seven public IPv4 addresses are three RDS instances plus four Aurora
  writers**, each publicly reachable and each holding one chargeable address.
  The nine managed secrets are those seven databases plus the two proxy secrets.
- **App compute predates the installation.** The workspace App was serving
  before this run existed, so it is disclosed separately and is the difference
  between the two totals README states. It was in a `Running` state throughout
  the window, which is the only state that bills; the figure is that state's
  rate and not a measurement of how much anyone used the app.

> [!NOTE]
> **This is a snapshot, and counts move.** The table is the shape one receipt
> sealed on 2026-08-25. Shapes change: the RDS fleet was `db.t4g.micro` until it
> was resized on 2026-08-21, and Round 1's RDS instance was deleted after that,
> which is why three instances stand rather than four.
> `server/cost_model.py` is authoritative for both the rates and the shape.

### The AWS rate sources

Public offer files, no authentication required. Each carries its own publication
timestamp, which is not the date it was read:

| Provider map | Published | Source |
| --- | --- | --- |
| Amazon RDS and RDS Proxy | 2026-08-18T00:11:58Z | [AmazonRDS us-west-2](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonRDS/current/us-west-2/index.json) |
| Amazon EC2 and EBS | 2026-08-19T16:58:43Z | [AmazonEC2 us-west-2](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/us-west-2/index.json) |
| Amazon VPC | 2026-07-24T15:42:25Z | [AmazonVPC us-west-2](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonVPC/current/us-west-2/index.json) |
| AWS Secrets Manager | 2025-08-28T15:38:04Z | [AWSSecretsManager us-west-2](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AWSSecretsManager/current/us-west-2/index.json) |

The Databricks rates come from `system.billing.list_prices` in your own
workspace, so they are not a published file this repository can link. The
[billing system tables
documentation](https://docs.databricks.com/aws/en/admin/system-tables/billing)
and the [pricing system tables
documentation](https://docs.databricks.com/aws/en/admin/system-tables/pricing)
describe both tables.

### What is redacted, and how

Anything published from a receipt follows the substitution convention already
used in [PRICING_DISCOVERY.md](../PRICING_DISCOVERY.md): AWS account IDs become
`111122223333`, workspace IDs become `1111222233334444`, Lakebase project UIDs
become `11111111-2222-3333-4444-555555555555`, and statement IDs are dropped
rather than faked. The table above needs none of those, because a count and a
public rate identify nobody. The receipt code `EECDD4D6` is the bout's session ID
truncated to its first four bytes and uppercased, which does not reverse to the
full identifier.

## Why this page quotes no totals

`tests/test_standing_cost.py` keeps README, `docs/BOOTSTRAP.md` and
`CONTRIBUTING.md` in agreement about the headline figures. It checks that the
three documents state the same numbers, that each total is the arithmetic of its
parts, that all three cite one receipt, and that none of them quietly drops the
caveat that the AWS half has no invoice behind it.

A fourth document quoting those figures without being in that plan would drift
the first time the meter moved, and drifting cost documents are the exact defect
that suite was built after. So this page carries method, provenance and inputs,
and points at README for the money.

If the totals are wanted here too, this file needs adding to `_COST_DOCUMENTS`
in `tests/test_standing_cost.py`, and it must then state all six `_COST_CLAIMS`
quantities in one of their recognised phrasings. That is a deliberate choice
rather than a formatting change, so it is left to the owner.

## Reproducing it

The rates and shape are in `server/cost_model.py`. The estimator that combines
them is `estimate_carrying_cost`, and `server/standing_cost.py` builds the
disclosure the app renders. `tests/test_cost_model.py` and
`tests/test_standing_cost.py` recompute every figure from the same two inputs, so
a rate change either moves the estimate and the expectation together or fails
loudly in both.
