# The Azure edition

The same six rounds, raced on Azure: Lakebase on an Azure Databricks workspace against
**Azure Database for PostgreSQL** (Flexible Server), with Azure's own integrations wherever a
round moves data. An installation is AWS or Azure for life. The Azure edition takes no AWS
account, key, CLI or API anywhere, and `./bootstrap.sh` refuses an AWS value in its inputs.

| Round | Azure's lane |
| --- | --- |
| 1 · Wake an idle app | Flexible Server has no automatic scale-to-zero, so its lane is checked before the bell and Lakebase is timed alone (RDS's rule). No server stands for it. |
| 2 · Change a schema safely | A point-in-time restore of the round's own server, with its firewall rules, then the identical migration and transaction. |
| 3 · Recover a deleted order | A point-in-time restore to the agreed recovery point, then the exact read. |
| 4 · Move lakehouse data into an app | An Azure Data Factory mapping data flow, cold at the bell, reads the Delta change from the installation's own ADLS account and writes it into the round's server. |
| 5 · Ready a pooled path | The round's General Purpose server's built-in PgBouncer, switched on at the bell: Azure's timed setup is the switch, the gate on its port 6432, and the raise to 10,100 clients. Then each lane's runner VM opens and holds exactly 10,000 clients, Lakebase's on its pooled endpoint, with AWS's runner harness byte for byte. |
| 6 · Move app data into the lakehouse | A Data Factory data flow, cold at the bell, extracts the checkout by a watermark column into a Delta table that Unity Catalog reads as an external table. |

## What you need

- **Tools:** `uv`, Node.js and `npm`, the Databricks CLI, the Azure CLI (`az`), Terraform 1.11 or
  newer, `python3` and `jq`. Not the AWS CLI.
- **An Azure identity, signed in with `az login --tenant <tenant>`,** that holds Contributor and User
  Access Administrator on the subscription or on the resource group. Terraform makes role
  assignments, and Contributor alone can't. The resource group must exist: the installation never
  creates or deletes it.
- **An Azure Databricks workspace (Premium)** in the same region, with:
  - a Unity Catalog metastore;
  - Lakebase available in the region;
  - a SQL warehouse;
  - a Databricks CLI profile that reaches it.
  The installer makes Unity Catalog storage credentials, external locations, a catalog and a
  service credential, so its identity needs those privileges on the metastore.

## The inputs

`.env.bootstrap`, beside `bootstrap.sh`. Nothing in it is a secret:

```dotenv
DATABRICKS_HOST=https://adb-<workspace-id>.<n>.azuredatabricks.net
DATABRICKS_PROFILE=<your CLI profile for that workspace>
DATABRICKS_WAREHOUSE_ID=<a SQL warehouse ID>
AZURE_TENANT_ID=<tenant GUID>
AZURE_SUBSCRIPTION_ID=<subscription GUID>
AZURE_RESOURCE_GROUP=<an existing resource group>
AZURE_REGION=<the workspace's region, e.g. westus2>
# Optional:
# AZURE_ZONE=1                       every server and runner's availability zone (1, 2 or 3)
# AZURE_POLICY_TAGS=name=value,...   tags your subscription's policy requires on every resource
# AZURE_LEASE_DATE_TAG=<tag name>    a tag the policy reads as a date, moved with the lease
# DATABRICKS_APP_NAME=lakebase-anti-demo
```

The edition comes from the host: an `azuredatabricks.net` workspace is the Azure edition.

## One command

```bash
./bootstrap.sh --apply --deploy-app --yes
```

It hands the run to `./antidemo azure-setup`, which resumes from wherever a stopped run left off.
State, variables and the servers' admin password are kept in `.anti-demo-azure/`, which is
gitignored. In order, it:

1. **Seals the installation's identity** and the region's published Databricks egress.
2. **Applies `infra/azure`:**
   - five Flexible Servers, each with only the operator's address and the Databricks egress on its
     firewall;
   - an ADLS account;
   - two Access Connectors (Unity Catalog's, and the App's);
   - a data factory on a managed virtual network, with private endpoints to its servers and the
     storage account;
   - Round 5's Key Vault, Service Bus and two runner VMs, which reach out through a NAT gateway
     (`round5_runners = false` leaves the VMs out, and the round with them);
   - the role assignments.
   A run that stops only because a new server was briefly busy is applied again for just what it
   missed.
3. **Approves the factory's private endpoints** on their targets.
4. **Builds Lakebase's seven projects** (six rounds and coordination), and **seeds the identical
   schema** on Lakebase and on each server that races on it.
5. **Makes the installation's Unity Catalog objects**, all on its own ADLS account, and **the App**.
6. **Seals each round:**
   - Round 4's synced table and Round 6's change feed;
   - the two data flows, each proven by one cold run before it is sealed;
   - Round 5's runners, installed over Run Command, its database roles on both lanes, and each
     lane's control secret in Key Vault. A later run reinstalls the runners whenever this tree's
     runner files differ from the ones they run.
7. **Deploys the App** with an `app.yaml` that binds no AWS secret.

**The App holds no Azure secret.** It reaches Azure Resource Manager through a Unity Catalog
service credential on its own Access Connector. That connector's managed identity has Contributor
on the resource group and nothing else. A metastore admin can mint the same token.

## What it costs

Standing Azure resources, at list prices in westus2 (eastus2 is the same on every line). This is
arithmetic, not a bill:

| What stands | Per day |
| --- | ---: |
| Servers for Rounds 2, 3, 4 and 6 (B2s) | $6.53 |
| Round 5's server (D2ds_v5) | $4.27 |
| Round 5's two runners (D8ls_v7) | $22.51 |
| Their NAT gateway and its address | $1.20 |
| Server storage | $0.61 |
| Service Bus Standard | $0.33 |
| **Azure resources** | **≈ $35.45** |

- The Databricks side comes on top: Lakebase, the App, and the SQL warehouse while it runs.
- A data flow bills per vCore-hour while a bout runs it, about $0.14–$0.21 a run.
- Round 5's runners carry no public IP: they reach out through one NAT gateway, and Run Command
  needs no inbound path. A subscription's policy can deny a NIC that carries a public IP without an
  NSG on the NIC itself, which Terraform's provider can't attach when it creates one.
- The runners are Ubuntu 24.04, and the install turns off its daily unattended upgrade, which
  restarted a runner's agent at a random time each morning. AWS's Amazon Linux runners don't
  upgrade by themselves either. A rebuild takes the current image.

## Removing it

```bash
./antidemo azure-cleanup        # lists what is there; removes nothing
./antidemo azure-cleanup --yes  # removes it, then proves ALL GONE
```

It removes, in reverse:
- each round's per-bout copies;
- the App and its source;
- Rounds 4 and 6's Databricks objects;
- the Lakebase projects;
- the Unity Catalog objects;
- the secret scope;
- the factory's pipelines, data flows and server linked services, which the installer made outside
  Terraform and which Data Factory won't let Terraform's own linked service go before;
- `infra/azure`.

It then proves nothing tagged with the run is left in the subscription before it archives
`.anti-demo-azure/`. The resource group stays: it was never the installation's.

- **It needs no settings exported.** It removes the installation sealed in `.anti-demo-azure/`, with
  the subscription, group and profile that installation sealed.
- **It can be run again.** A removal that stopped part way picks up where it left off: what an
  earlier run removed is skipped.

## Testing it

- **The App's front door takes a Databricks sign-in, not an Azure one.** The installer's CLI profile
  may sign in with `az` (Entra ID), which the workspace accepts. The Databricks Apps proxy refuses
  that token (401, or a redirect to sign-in). To script against the App (its API, `databricks apps
  logs`, a browser), make an OAuth profile once: `databricks auth login --host <workspace>
  --profile <name>`.
- **The release bar's scripts run against it** with Azure's one opponent:

  ```bash
  ANTI_DEMO_APP_URL=<app url> ANTI_DEMO_PROFILE=<oauth profile> \
  CHAOS_COMPETITOR=azure_postgres .venv/bin/python scripts/release_bar/chaos.py
  .venv/bin/python scripts/release_bar/azure_leftovers.py . --wait 1800
  ```

  `azure_leftovers.py` is the `leaks` step: no Round 2 or 3 copy left standing, and no data flow
  run still going.
- **The rest of the bar** has an Azure script where AWS's needs AWS:
  - `backup`: `azure_backup.py . ROUND OUT_DIR` runs a full-proof bout of Round 2, 3 or 5 while
    Azure takes an on-demand backup of its source, then deletes the backup. Azure takes none of a
    Burstable server (`CustomerOnDemandBackupCannotBePerformedOnBurstableServer`), and Rounds 2 and
    3's sources are B2s, so on them it reports inconclusive. Round 5's server takes one, and
    PgBouncer's switch waits behind it, so Azure's lane is slower, on its own clock.
  - `lease`: `azure_lease_check.py .` checks that the App keeps every Terraform-made resource's
    `expires-at` alive. Terraform's half is a plain `terraform plan` of `infra/azure`, which must
    propose nothing.
  - `restart` and `crash` are `restart.py` with `--competitor azure_postgres`. Its redeploy is
    `./bootstrap.sh --apply --deploy-app --yes`, which resumes a ready installation and deploys
    only the App.
  - Crash Rounds 2 and 3 as well as 4 and 6. A copy finishes in about seven minutes, before a
    redeploy lands, so only `--stop-start` lands inside one.
- **Round 4 reopens three to four minutes after a bout.** Putting its row back takes one cold Data
  Factory run, so every Round 4 Prepare after a restart cut that short takes as long. A test that
  arms Round 4 beside other rounds and rings them all together can see the others' armed cards
  expire (180 s) while Round 4 prepares.
