# Archived rate audit: 2026-08-20

> [!NOTE]
> **This is a dated derivation, not current state.** It records how the rates in
> `server/cost_model.py` were checked against provider sources on 2026-08-20,
> and it prices the installation as it was shaped that day. Every deployment
> count and every dollar subtotal below has since been superseded.
>
> For what the installation costs now, read [the cost box in
> README.md](README.md#cost-and-safety). For how those figures are produced, read
> [docs/PRICING.md](docs/PRICING.md). For the live rates and the sealed shape,
> read `server/cost_model.py`.

This audit exists to answer one question a reader is entitled to ask: are the
unit rates real, and does the arithmetic hold? It is kept because it is the only
published evidence for that, and because a rate check does not go stale the way a
resource count does.

## A note on the identifiers below

Every query recorded here was run against a real workspace and a real Lakebase
project, and every rate, quantity, SKU and computed figure is the value that came
back. The identifiers naming *whose* workspace and project those were are not,
because this repository is public.

Four substitutions, applied consistently throughout:

| Real thing | Written here as | Why this shape |
| --- | --- | --- |
| AWS account ID | `111122223333` | AWS's own reserved documentation account, so it is recognisably not anybody's |
| Databricks workspace ID | `1111222233334444` | keeps `workspace_id`'s shape, a numeric string, so the filter predicates still read correctly |
| Lakebase project UID | `11111111-2222-3333-4444-555555555555` | keeps the UUID shape that distinguishes a project UID from a project's friendly name |
| SQL statement ID | `[statement ID redacted]` | a statement ID's only job here is to attest that a query ran and succeeded. A fake UUID would look queryable and would not be, so the attestation is kept and the false affordance is not |

**None of these placeholders is queryable.** They are not obfuscated real values.
Substituting your own workspace and project is what makes the queries here
runnable. No result below depends on the *value* of an identifier, only on every
row having been filtered to one workspace and one project.

The rest of the repository does not share one placeholder spelling, so do not
read a single convention into a grep: `docs/iam/` uses the named form
`<AWS_ACCOUNT_ID>` because those policy documents are meant to be edited, the
tests and `infra/aws/terraform.tfvars.example` use `123456789012`, and
`.env.example` uses `000000000000` as a replace-me sentinel that
`tests/test_api.py` asserts never reaches a served app configuration. None of the
four is a real account.

## Databricks rates: measured usage against posted prices

Databricks figures come from two Delta Sharing system tables. Both were profiled
live; the schema below is the one both discovery passes returned.

**Sources**, both fetched 2026-08-20:

- [system.billing.usage](https://docs.databricks.com/aws/en/admin/system-tables/billing) (page updated 2026-07-23)
- [system.billing.list_prices](https://docs.databricks.com/aws/en/admin/system-tables/pricing) (page updated 2025-02-04)

```text
system.billing.usage
workspace_id            string     Workspace this usage was associated with
sku_name                string     Exact rate-card lookup key
cloud                   string     AWS, AZURE or GCP
usage_start_time        timestamp  Provider metering interval start, UTC
usage_end_time          timestamp  Provider metering interval end, UTC
usage_date              date       Bounded scan key
usage_unit              string     DBU, DSU and others
usage_quantity          decimal(38,18)  Billable units, not dollars
usage_metadata          struct<...project_id, branch_id, endpoint_id...>
record_type             string     ORIGINAL, RETRACTION or RESTATEMENT
ingestion_date          date       Lag evidence, not a usage boundary
billing_origin_product  string     LAKEBASE selects Lakebase-originated usage
usage_type              string     COMPUTE_TIME, STORAGE_SPACE and others

system.billing.list_prices
price_start_time  timestamp
price_end_time    timestamp
sku_name          string
cloud             string
currency_code     string
usage_unit        string
pricing           struct<default, promotional.default, effective_list.default>
```

Neither table exposes a partition block. `usage_date` is the documented bounded
scan key for `usage` and is treated as the effective partition column; price
scans are restricted to `cloud = 'AWS'`, the exact observed SKUs, and the active
price interval.

Three assumptions are load-bearing and worth stating rather than burying:

- `billing_origin_product = 'LAKEBASE'` is what keeps unrelated products out.
  The same workspace also emits `APPS`, `DATABASE`, `JOBS`, `LAKEFLOW_CONNECT`,
  `NETWORKING` and `SQL` rows, so workspace-only attribution would overstate
  Lakebase substantially.
- `usage_metadata.project_id` is the exact project UID, which is distinct from
  the project's friendly name. Scoping to it is what stops other users of the
  same workspace being attributed to this demo.
- Correction rows stay in the sum. Filtering to `record_type = 'ORIGINAL'` would
  discard retractions and restatements, which is not what a reconciliation wants.

### The posted rates

```text
ENTERPRISE_DATABASE_SERVERLESS_COMPUTE_US_WEST_OREGON | AWS | USD | DBU
  2026-07-01 -> open | list 0.520000 | promotional 0.260000 | effective 0.260000

ENTERPRISE_DATABRICKS_STORAGE_US_WEST_OREGON | AWS | USD | DSU
  2024-09-23 -> open | list 0.023000 | promotional NULL | effective 0.023000
```

The active price table confirms the 50% compute promotion and does **not** encode
an end date for it, so the app must not claim a specific cutoff from this table
alone; it tells the presenter to revalidate instead. Storage publishes no
promotional value, so its effective rate equals list. Negotiated contract rates
are excluded by design: `list_prices` is a published-list table.

### The reconciliation, and one arithmetic finding

Scoped to workspace `1111222233334444` and project UID
`11111111-2222-3333-4444-555555555555`, bounded to the 2026-08-19
America/Chicago day. Statement `[statement ID redacted]` succeeded at
2026-08-20T03:47Z.

```text
Lakebase compute | 131 rows | 1.934750 DBU | posted through 2026-08-20T01:20Z
  normal list:   1.934750 x $0.520000 = $1.006070
  current promo: 1.934750 x $0.260000 = $0.503035

Lakebase storage |  90 rows | 0.037580 DSU | posted through 2026-08-20T01:00Z
  list/effective: 0.037580 x $0.023000 = $0.000864

Posted total at these watermarks:
  normal list:                 $1.006934
  current effective promotion: $0.503899
```

The reconciliation sums `usage_quantity` inside each active price interval before
multiplying, casts the aggregate to `DECIMAL(29,18)` and the rate to
`DECIMAL(8,6)`, and only then multiplies. Both steps matter. Multiplying per row
rounds every row; multiplying a raw `DECIMAL(38,18)` aggregate by a raw
`DECIMAL(38,18)` rate still truncates the result to six decimal places. The
original per-row-first check produced $0.641367 where exact-decimal arithmetic
gives $0.641362, and this ordering is what removed that discrepancy.

Three independent statements returned the same quantities, intervals,
attribution and totals, so there is no rate, unit, promotion or arithmetic drift
in the figures above.

**What the watermark means.** These are exact for the rows posted at query time
and are not a completed daily invoice. Ingestion lag is not encoded in the rows,
so a total must carry its `MAX(usage_end_time)` watermark and must never be
labelled final. During this audit the compute watermark trailed query time by
about 2 hours 27 minutes and storage by about 2 hours 47 minutes, and repeated
runs grew the total purely because more rows had posted.

**Not verified here:** contract discounts, invoice credits, invoice timing, and
per-bout attribution. Posted rows carry no per-bout allocation.

### One system table had nothing to offer

`system.storage.table_metrics_history` was profiled as a possible source of
Delta storage bytes. A seven-day bounded lookup for the sealed Round 4 and
Round 6 table IDs returned **0 rows**, so the receipt must not invent Delta
storage bytes from it. It uses a direct table-size observation where one is
available and labels storage unavailable otherwise. No current public AWS
system-table page for this table was found in the 2026-08-20 docs sitemap, so
the live schema was the source of truth for its existence and types.

## AWS rates: published rate cards, and no invoice

Every Oregon unit rate the app displays was checked against the current public
provider rate card. Identity checked: AWS account `111122223333`, region
`us-west-2`.

> [!IMPORTANT]
> **The AWS half has never met an invoice.** Cost Explorer
> (`ce:GetCostAndUsage`, and `ce:GetDimensionValues` as probed here) and
> authenticated Price List API access (`pricing:GetProducts`) are both denied to
> this role. Public AWS price files validate the rates and CloudWatch and
> CloudTrail validate the formula inputs, but no AWS figure anywhere in this
> repository has been reconciled against a bill. Every AWS dollar below is an
> estimate. The receipts say the same thing structurally: every AWS component
> carries `provenance: assumed`, while every Databricks component carries
> `provenance: measured`.

| Component | Published Oregon rate | Provider evidence |
| --- | ---: | --- |
| Aurora Serverless v2 compute | `$0.12/ACU-hour` | AmazonRDS SKU `QR6MZKHMCWCPDAAM` |
| Aurora standard storage | `$0.10/GB-month` | AmazonRDS SKU `XN6AM4AHVSYQ6BFC` |
| Aurora standard I/O | `$0.20/million requests` | AmazonRDS SKU `FP5XU5WFCUVXQXF5` |
| Aurora excess backup | `$0.021/GB-month` | AmazonRDS SKU `JZGHDWS5VTC87BRN` |
| RDS PostgreSQL `db.t4g.micro` | `$0.016/instance-hour` | AmazonRDS SKU `CT79XNCJJGH56FA8` |
| RDS gp3 | `$0.115/GB-month` | AmazonRDS SKU `W2B5U7TQ8BTUD7C8` |
| RDS excess backup | `$0.095/GB-month` | AmazonRDS SKU `PAHDKG6EF4XSHYXC` |
| RDS Proxy | `$0.015/ACU- or vCPU-hour` | Amazon RDS Proxy Oregon pricing map |
| Secrets Manager storage | `$0.40/secret-month` | AWSSecretsManager SKU `DWJP9S4V3HP98UNC` |
| Secrets Manager API calls | `$0.05/10,000 requests` | AWS Secrets Manager public pricing |
| Neutral `m6i.large` runner | `$0.096/instance-hour` | AmazonEC2 Oregon Linux On-Demand meter |
| Runner gp3 | `$0.08/GB-month` | AmazonEBS Oregon gp3 meter |
| Public IPv4 | `$0.005/address-hour` | AmazonVPC SKU `NBHXEKTE88TJDDQF` |

All displayed rates matched the public Oregon values. The provider maps carry
their own publication timestamps, which are not the same thing as the date this
audit read them:

| Provider map | Published | Source |
| --- | --- | --- |
| Amazon RDS and RDS Proxy | 2026-08-18T00:11:58Z | [AmazonRDS us-west-2 offer file](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonRDS/current/us-west-2/index.json) |
| Amazon EC2 and EBS | 2026-08-19T16:58:43Z | [AmazonEC2 us-west-2 offer file](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/us-west-2/index.json) |
| Amazon VPC | 2026-07-24T15:42:25Z | [AmazonVPC us-west-2 offer file](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonVPC/current/us-west-2/index.json) |
| AWS Secrets Manager | 2025-08-28T15:38:04Z | [AWSSecretsManager us-west-2 offer file](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AWSSecretsManager/current/us-west-2/index.json) |

The app states that the full rate set was checked on 2026-08-20 rather than
presenting that check time as a provider publication time.

### The shape this audit priced, and why it no longer applies

On its audit date the installation was: Aurora PostgreSQL 17.10 with one
`db.serverless` writer at 0 to 2 ACUs and a 300-second auto-pause; RDS PostgreSQL
17.10 Single-AZ `db.t4g.micro` with 20 GiB gp3; one Linux `m6i.large` runner with
a 20 GiB gp3 root volume and one public IPv4; and four static secrets across both
installed AWS lanes.

> [!WARNING]
> **Superseded, and not only on the RDS line.** The RDS fleet was resized to
> `db.t4g.medium` at 2026-08-21T14:48:36Z and Round 1's RDS instance was deleted
> afterwards, and the installation has grown since: it now carries more Aurora
> clusters, more RDS instances and more secrets than the snapshot above. Nothing
> here may be quoted as the current shape, and the same applies to every dollar
> subtotal below. Each one prices that smaller installation over a bounded
> window, so none is comparable with the per-day standing-cost figure README
> publishes. `server/cost_model.py` holds the live rates and the sealed shape.

### Formula replay

The formulas, which are what survives the shape changing:

```text
Aurora compute = SUM(average ACU x interval seconds) / 3600 x $0.12
Aurora storage = consumed GB-month x $0.10
Aurora I/O     = (billed reads + writes) / 1,000,000 x $0.20
Aurora backup  = excess billed backup GB-month x $0.021

RDS compute = max(provider billable seconds, 600-second minimum) / 3600 x rate
RDS gp3     = provisioned GB-month x $0.115
RDS backup  = regional excess backup GB-month x $0.095

Secrets     = secret-months x $0.40 + API requests / 10,000 x $0.05
Runner      = billable seconds / 3600 x $0.096
Runner gp3  = provisioned GB-month x $0.08
Public IPv4 = billable seconds / 3600 x $0.005
```

Replayed against observable meters for the bounded 2026-08-19 Chicago day, on the
shape described above:

| Component | Basis | Estimate (USD) |
| --- | --- | ---: |
| Aurora compute | CloudWatch ACU integration | 0.618361 |
| Aurora storage | integrated `VolumeBytesUsed` | 0.000148 |
| Aurora I/O | billed read and write counts | 0.016009 |
| RDS compute | elapsed billable seconds | 0.352818 |
| RDS gp3 | 20 GiB prorated | 0.070441 |
| Runner compute | elapsed seconds | 1.188107 |
| Runner gp3 | 20 GiB prorated | 0.027502 |
| Two database public IPv4s | 2 x 79,384s | 0.220511 |
| Runner public IPv4 | elapsed seconds | 0.061881 |
| Two master secrets | prorated secret-months | 0.024501 |
| Two proxy secrets | prorated secret-months | 0.005101 |
| **Observable subtotal** | exact sum; the components above are rounded for display | **2.585378** |

Pending or excluded rather than assumed to be zero: RDS Proxy billed duration,
Secrets Manager billed API-request quantity, accepted cross-AZ runner-to-database
bytes at `$0.01/GB`, excess database backup storage, CloudWatch API charges
beyond the free tier, and any usage after the stated watermark.

Two receipt omissions were found by this replay and corrected without inventing
quantities: each selected AWS database gets one public IPv4 line, and Round 5
gets one cross-AZ runner-to-database transfer line at `$0.01/GB` whose quantity
stays pending because the accepted-traffic flow log was in failed delivery
status.

### RDS Proxy: a floor is not a lifetime

The original receipt always charged the 10-minute minimum, which is only valid
when the Proxy's provider lifetime is at most ten minutes:

```text
Aurora Proxy = $0.015 x 8 ACU  x max(600, billable_seconds) / 3600
RDS Proxy    = $0.015 x 2 vCPU x max(600, billable_seconds) / 3600
```

CloudTrail reproduced seven create-to-delete-request spans for the run prefix:
1,296, 605, 685, 625, 409, 205 and 681 seconds. Applying the 600-second floor
gives exactly 5,092 priced seconds and 11.315556 ACU-hours. The 681-second span
is the successful browser bout and independently exercises the above-minimum
branch at $0.022700, which the earlier 409-second floor case could not.

The app now presents `$0.020` for Aurora or `$0.005` for RDS explicitly as the
**10-minute minimum**, keeps the final lifetime pending, and carries a tested
lifetime-aware calculation for later reconciliation. CloudTrail deletion-request
time is control-plane evidence, not a provider billing record, so the strict
reconciliation excludes Proxy duration entirely rather than treating a
control-plane span as a billed one. An earlier non-strict replay included
$0.169733 of CloudTrail-derived Proxy cost; that is why the two subtotals in this
document differ.

CloudTrail was also removed as a source for the Secrets Manager API-request
count. An ownership-aware audit includes successful calls, `ResourceNotFound` and
`AccessDenied` attempts, and AWS-service-invoked calls, and public pricing does
not document how those categories map to the billed request meter. CloudTrail
validates that activity happened, not the invoice quantity.

## What this audit does not license

Do not present any subtotal here as a final invoice, a single-bout cost, or a
savings comparison. It is a bounded-window carrying-cost estimate on a shape that
no longer exists, and its AWS half has no invoice behind it.
