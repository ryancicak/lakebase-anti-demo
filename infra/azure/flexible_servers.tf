# Azure Database for PostgreSQL Flexible Servers: r2, r3, r4, r6 (B2s), r5 (D2ds_v5)
# Each server is public-access, no HA, no geo-redundant backup. All servers are PostgreSQL 17.
# The admin password reaches Azure through a write-only argument, so Terraform sends it at create
# and never records it in state.

resource "azurerm_postgresql_flexible_server" "by_round" {
  for_each = local.v7_round_keys

  name                          = local.round_server_names[each.key]
  resource_group_name           = data.azurerm_resource_group.target.name
  location                      = var.location
  version                       = "17"
  administrator_login           = "antidemo_admin"
  storage_mb                    = 32768
  backup_retention_days         = 7
  geo_redundant_backup_enabled  = false
  zone                          = var.zone # pinned: see the variable
  public_network_access_enabled = true

  # Use the appropriate SKU based on round. r5 needs GeneralPurpose for PgBouncer;
  # others use Burstable.
  sku_name = each.key == "r5" ? var.round5_server_sku : var.flexible_server_sku

  # Write-only (Terraform >= 1.11): sent at create, never stored. Azure refuses to create a
  # password-auth server without it, which `terraform validate` cannot see. Bump the version to
  # rotate; the value itself is never compared.
  administrator_password_wo         = var.admin_password
  administrator_password_wo_version = var.admin_password_version

  tags = merge(local.required_tags_with_lease, {
    "anti-demo-round" = each.key
  })

  lifecycle {
    # The app moves the lease tags while in use; a moved lease is not drift.
    ignore_changes = [tags]
  }
}

# Round 5 specific server parameter: PgBouncer baseline is OFF, with max_client_conn
# pre-set if Azure allows it while off. The spike (Finding 2) showed:
# - pgbouncer.enabled defaults to false, and switching it on takes ~9s (timed).
# - Setting max_client_conn while off is refused (ServerParameterToCMSBlockedUpdateForDisabledPgBouncer).
# - Raising it from 5000 to 10100 after the switch is a second timed mutation (~6.4s). Exactly
#   10000 admits 9,999 clients, because Azure holds a slot.
# So r5's Terraform only sets pgbouncer.enabled = false (the baseline), leaving the
# two on-bell mutations to the installer's setup harness.

resource "azurerm_postgresql_flexible_server_configuration" "round5_pgbouncer_off" {
  name      = "pgbouncer.enabled"
  server_id = azurerm_postgresql_flexible_server.by_round["r5"].id
  value     = "false"

  # Explicitly OFF at baseline, proved before every warm run (server/round5_warm.py).
  # During the bout, the app turns it on and raises max_client_conn; both are timed.
}

# Fire up all servers in parallel, but do not accept connections until firewall rules are ready.
# The installer connects once firewall rules are in place.
