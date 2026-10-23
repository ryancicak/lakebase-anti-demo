locals {
  # Resource naming: derive a stable digest of the installation_id (or run_id if not given)
  # to keep independently installed copies from colliding without exposing the full ID.
  # Similar to infra/aws, include round-specific suffixes for per-round resources.
  stable_id_base = var.installation_id != null ? var.installation_id : var.run_id
  v7_round_keys  = toset(["r2", "r3", "r4", "r5", "r6"])

  # An 80-bit digest keeps independently installed copies in the same subscription/region
  # from colliding. Including the round key in the digest and suffix makes ownership exact.
  v7_round_slugs = {
    for round in local.v7_round_keys :
    round => "i${substr(sha256("${trimspace(local.stable_id_base)}:${round}"), 0, 20)}-${round}"
  }

  # The App's own identity (access_connector.tf), digested like a round's so copies never collide.
  app_slug = "i${substr(sha256("${trimspace(local.stable_id_base)}:app"), 0, 20)}-app"

  v7_round_resource_names = {
    for round, slug in local.v7_round_slugs :
    round => "${substr(var.name_prefix, 0, 12)}-${substr(var.run_id, 0, 8)}-${slug}"
  }

  # Storage account names must be globally unique, 3-24 chars, lowercase alphanumeric only.
  # Derive from a short digest of the installation ID.
  storage_account_name = "st${substr(sha256(local.stable_id_base), 0, 20)}${substr(var.run_id, 0, 2)}"

  # Deterministic round slugs for database names, firewall rules, etc.
  # The Azure edition does not split by cloud like v7 AWS does; there is one competitor.
  round_server_names = {
    for round in local.v7_round_keys :
    round => "server-${local.v7_round_resource_names[round]}"
  }

  # Azure compares tag names without case, so each tag is emitted once, in one spelling. The
  # subscription's own policy tags come first, so they can never replace the installation's.
  required_tags = merge(var.policy_tags, {
    "anti-demo-run-id" = var.run_id
    "owner"            = trimspace(var.owner)
    "managed-by"       = "terraform"
  })

  # expires-at is a lease, not an ownership tag: the app moves it forward while the installation
  # is in use (server/lease.py), with the optional date tag beside it. Tags are written at
  # creation and never reconciled after (`lifecycle { ignore_changes = [tags] }` on every tagged
  # resource), because Terraform cannot name a configurable tag key in ignore_changes, and a moved
  # lease must not be drift.
  lease_tags = merge(
    { "expires-at" = var.expires_at },
    var.lease_date_tag == null ? {} : { (var.lease_date_tag) = substr(var.expires_at, 0, 10) },
  )
  required_tags_with_lease = merge(local.required_tags, local.lease_tags)

  # Firewall rule names: deterministic, keyed by CIDR.
  # Convert CIDR to start/end IP addresses for Azure (Azure firewall rules need explicit IPs).
  firewall_rules_by_cidr = {
    "operator" = {
      cidrs       = [var.operator_cidr]
      description = "Operator /32"
    }
    "databricks" = {
      cidrs       = var.databricks_egress_cidrs
      description = "Databricks serverless egress"
    }
  }

  # Container names for ADLS: each must be >= 3 chars and globally unique within the account.
  # Deterministic, derived from round/purpose.
  container_names = {
    r4_source        = "r4source"
    r6_history       = "r6history"
    change_feed_dest = "change-feed"
    uc_managed_loc   = "uc-managed"
  }

  # Data Factory IR name: must allow only letters, digits and hyphens (no underscores).
  adf_ir_name = "ir-${local.v7_round_slugs["r4"]}"

  # Service Bus queues: one per runner
  service_bus_queues = {
    "r5-runner-0" = "queue-r5-runner-0"
    "r5-runner-1" = "queue-r5-runner-1"
  }

  # Key Vault name: 3-24 chars, alphanumeric and hyphens only
  key_vault_name = "kv-${substr(local.v7_round_slugs["r5"], 0, 20)}"

  # Resource group: pass in as a data source, never created or deleted
  resource_group_id = "/subscriptions/${data.azurerm_client_config.current.subscription_id}/resourceGroups/${var.resource_group_name}"
}

# Current Azure subscription and client context
data "azurerm_client_config" "current" {}

# Pre-existing resource group: Terraform must never create or delete it
data "azurerm_resource_group" "target" {
  name = var.resource_group_name
}
