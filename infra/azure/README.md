# Azure Terraform Root Module

This module provisions Azure resources for the Lakebase anti-demo Azure edition, one region, one resource group.

## What This Module Builds

- **PostgreSQL Flexible Servers** (5): r2, r3, r4, r6 (B2s), r5 (D2ds_v5, GeneralPurpose for PgBouncer)
  - PostgreSQL 17, 32 GiB storage, public access, no HA, 7-day backup retention
  - Firewall rules: operator /32 and the region's Databricks egress CIDRs, on every server
  - No Data Factory range: data flows don't leave from the published DataFactory ranges, so
    Rounds 4 and 6 reach their servers through the factory's managed private endpoints
  - r5 PgBouncer baseline: off (enabled at the bell by the setup harness)
- **ADLS Gen2 Storage Account**: HNS on, LRS, shared-key access OFF (managed identity only)
  - Containers: r4source, r6history, change-feed destination, UC managed location
- **Databricks Access Connector**: system-assigned identity for Unity Catalog
- **The App's Access Connector**: the deployed App's Azure identity, separate from Unity Catalog's.
  The App reaches ARM through a Unity Catalog service credential on it, which mints a short-lived
  token: no app registration or client secret
- **Azure Data Factory**: a managed virtual network; a regional integration runtime inside it
  (TTL 0, so every bout's run is a cold start); managed private endpoints to the r4 and r6 servers
  and to the ADLS account's dfs endpoint, each approved on its target by the installer; a linked
  service to ADLS through that runtime
- **VNet, Subnet, NSG**: for Round 5 runners (NSG: no inbound rules, outbound all allowed)
- **Two Linux VMs** (`Standard_D8ls_v7`, Ubuntu 24.04): Round 5 resident runners with managed identities
- **Key Vault** (standard, purge protection off): holds runner secrets
- **Service Bus Namespace** (standard) with two queues: ordered control traffic per runner (FIFO equivalent)

## Module Inputs

| Variable | Type | Required | Default | Notes |
| --- | --- | --- | --- | --- |
| `location` | string | yes | — | Azure region, lowercase form (e.g., `eastus2`) |
| `resource_group_name` | string | yes | — | Pre-existing resource group; Terraform never creates/deletes |
| `run_id` | string | yes | — | 3-24 lowercase alphanumerics/hyphens, unique per run |
| `owner` | string | yes | — | Owner email, on every resource |
| `expires_at` | string | yes | — | RFC 3339 UTC timestamp, as on AWS; the app moves it while in use |
| `policy_tags` | map(string) | no | `{}` | Tags the subscription's own policy requires on every resource |
| `lease_date_tag` | string | no | `null` | Name of a tag that carries the lease's date (YYYY-MM-DD), if the policy expects one |
| `operator_cidr` | string | yes | — | Operator's public IP /32 (e.g., `203.0.113.10/32`) |
| `databricks_egress_cidrs` | list(string) | no | `[]` | Databricks published egress ranges (width /23 allowed) |
| `name_prefix` | string | no | `lakebase-anti-demo` | Resource name prefix |
| `installation_id` | string | no | `null` | Stable installation ID for naming; null uses run_id digest |
| `zone` | string | no | `"1"` | Availability zone for every server and both runners. Pinned, because a size can be refused in one zone and offered in the others. Null leaves placement to Azure, for a region without zones |
| `key_vault_authorization` | string | no | `"rbac"` | `"rbac"`, or `"access_policy"`, which a Contributor can set without role assignments |
| `flexible_server_sku` | string | no | `B_Standard_B2s` | SKU for r2/r3/r4/r6 |
| `round5_server_sku` | string | no | `GP_Standard_D2ds_v5` | SKU for r5 (must be GeneralPurpose) |
| `runner_vm_sku` | string | no | `Standard_D8ls_v7` | SKU for runner VMs (8 vCPU, 16 GiB); `Standard_D8ls_v6` when a zone has no v7 left |
| `runner_ssh_public_key` | string | yes | — | SSH public key for runner authentication |
| `manage_role_assignments` | bool | no | `true` | Create role assignments; false outputs pending assignments |

## What Is NOT in Terraform

- **Resource group**: pre-existing, read-only via `data.azurerm_resource_group`
- **Data flows and pipelines**: created by the installer (deterministic names, idempotent)
- **Round 4 Delta source table**: created by the installer (table location determines ADLS prefix)
- **Lakebase projects, endpoints, secrets**: all provisioned by the installer
- **Azure service principal**: provisioned outside (environment variables only)

## Passwords

Admin passwords for PostgreSQL servers stay out of Terraform state. They reach Azure through
azurerm's write-only `administrator_password_wo` (Terraform 1.11 or later), sent at create and
never stored; `admin_password` is an ephemeral variable. Bump `admin_password_version` to rotate.

## RBAC Role Assignments

When `manage_role_assignments = true` (default):
- ADF identity and Access Connector identity → Storage Blob Data Contributor on storage
- The App's Access Connector identity → Contributor on the resource group
- Each runner identity → Key Vault Secrets User on Key Vault
- Each runner identity → Service Bus Data Receiver/Sender on its queue

When `manage_role_assignments = false`:
- Terraform outputs the exact role assignments (principal IDs, role names, scopes)
- Administrator creates them manually, e.g.:
  ```bash
  az role assignment create --assignee <principal-id> --role "Storage Blob Data Contributor" --scope <storage-account-id>
  ```

This is necessary in subscriptions where the installer has Contributor but not User Access Administrator.

## Tags

Every resource carries:
- `anti-demo-run-id`: run ID
- `owner`: owner email
- `managed-by`: "terraform"
- `expires-at`: the lease, an RFC 3339 timestamp the app moves forward while the installation is
  in use (`server/lease.py`)
- each of `policy_tags`, as given, and the `lease_date_tag`, if set, holding the lease's date

Azure compares tag names without case, and refuses a request naming one tag twice in two cases
where a policy reads it. So each tag is written once, in one spelling: `policy_tags` cannot
redefine the installation's own, name one tag twice, or name the `lease_date_tag`.

Tags are written at creation and never reconciled after (`ignore_changes = [tags]`), so a moved
lease is not drift.

## Usage

```bash
cd infra/azure
terraform init -backend-config=path=<generation-directory>/.terraform
terraform fmt -recursive
terraform validate
terraform plan -var-file=<tfvars-file>
terraform apply -var-file=<tfvars-file>
```

## Testing

```bash
cd infra/azure
terraform fmt -recursive        # Must pass
terraform init -backend=false -input=false
terraform validate               # Must pass; all resources must be valid
```
