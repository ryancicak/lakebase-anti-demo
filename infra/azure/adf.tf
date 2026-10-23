# Azure Data Factory for Rounds 4 and 6
# - Managed integration runtime in the region with TTL 0 (cold start each bout)
# - Linked service to ADLS using managed identity (no keys)
# - Data flows and pipelines are NOT in Terraform; the installer creates them.
#   This is by design: Round 4's table is not known until the installer runs,
#   and pipelines are idempotent via deterministic names in the setup code.

resource "azurerm_data_factory" "installation" {
  name                = "adf-${local.v7_round_slugs["r4"]}"
  resource_group_name = data.azurerm_resource_group.target.name
  location            = var.location
  identity {
    type = "SystemAssigned"
  }

  # Data flows do not leave from the published DataFactory.<region> ranges (Microsoft's
  # data-access-strategies doc; three Phase 0 runs could not reach a server that admitted all 12
  # eastus2 ranges). Their documented private route to Flexible Server is a managed virtual
  # network with managed private endpoints, so the factory has one and the servers' firewalls
  # admit no Data Factory range at all.
  managed_virtual_network_enabled = true

  tags = merge(local.required_tags_with_lease, {
    "anti-demo-purpose" = "rounds-4-6-data-mover"
  })

  lifecycle {
    ignore_changes = [tags]
  }
}

# Managed integration runtime: General Purpose, 8 cores, TTL 0 (no warm state).
# Every bout's data flow is a cold start, matching the design (6.4).
# azurerm 5.10 uses azurerm_data_factory_integration_runtime_azure for managed IR.
resource "azurerm_data_factory_integration_runtime_azure" "regional" {
  name             = local.adf_ir_name
  data_factory_id  = azurerm_data_factory.installation.id
  location         = var.location
  compute_type     = "General"
  core_count       = 8
  time_to_live_min = 0 # TTL 0: every bout's run is a cold start
  # Inside the factory's managed VNet, so its data flows reach the servers and the storage account
  # through the managed private endpoints below. A managed-VNet cold start is slower (Phase 0:
  # about 7 min to the sink against about 4 min on the public runtime), and that is Azure's time.
  virtual_network_enabled = true
}

# One managed private endpoint per thing a data flow touches: Round 4's target server, Round 6's
# source server, and the installation's ADLS account. Each lands Pending on its target; the
# installer approves it there through ARM (Phase 0: 54 s on a server, 13 s on the account), which
# Terraform's provider does not do.
resource "azurerm_data_factory_managed_private_endpoint" "round_server" {
  for_each = toset(["r4", "r6"])

  name               = "mpe-${each.key}-postgres"
  data_factory_id    = azurerm_data_factory.installation.id
  target_resource_id = azurerm_postgresql_flexible_server.by_round[each.key].id
  subresource_name   = "postgresqlServer"
}

resource "azurerm_data_factory_managed_private_endpoint" "adls" {
  name               = "mpe-adls-dfs"
  data_factory_id    = azurerm_data_factory.installation.id
  target_resource_id = azurerm_storage_account.installation.id
  subresource_name   = "dfs"
}

# Linked service to ADLS Gen2 (AzureBlobFS) using the factory's managed identity. Data flows read and
# write Delta through this type with the account's dfs URL; the S-Z4 and S-Z5 spikes ran on exactly
# this shape (with an account key, since the spike group could not grant the identity a role).
resource "azurerm_data_factory_linked_service_data_lake_storage_gen2" "installation_adls" {
  name                     = "ls-adls-installation"
  data_factory_id          = azurerm_data_factory.installation.id
  use_managed_identity     = true
  url                      = azurerm_storage_account.installation.primary_dfs_endpoint
  integration_runtime_name = azurerm_data_factory_integration_runtime_azure.regional.name
}

# Data flows and pipelines are created by the installer's setup code (app.py and server/lifecycle.py)
# after the Round 4 source table exists. They are deterministically named, so Terraform does not
# manage them. The installer is responsible for their lifecycle: create, park (cancel runs), and delete.
#
# Round 4 data flow: reads the Delta source table (versionAsOf) and writes to r4 PostgreSQL.
# Round 6 data flow: reads r6 PostgreSQL incrementally and appends to Delta in ADLS.
