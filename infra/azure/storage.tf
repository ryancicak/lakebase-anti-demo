# ADLS Gen2 storage account (StorageV2, HNS on, LRS, shared key OFF)
# Containers: r4source, r6history, change-feed destination, UC managed location.
# All containers >= 3 characters as required by Azure.

resource "azurerm_storage_account" "installation" {
  name                       = local.storage_account_name
  resource_group_name        = data.azurerm_resource_group.target.name
  location                   = var.location
  account_tier               = "Standard"
  account_replication_type   = "LRS"
  https_traffic_only_enabled = true
  min_tls_version            = "TLS1_2"
  is_hns_enabled             = true      # Enable hierarchical namespace (Data Lake Gen2)
  shared_access_key_enabled  = false     # Shared-key access is OFF (managed identity only)
  public_network_access      = "Enabled" # Needed for DataFactory and external access

  tags = merge(local.required_tags_with_lease, {
    "anti-demo-purpose" = "installation-storage"
  })

  lifecycle {
    ignore_changes = [tags]
  }
}

# Containers for the installation
resource "azurerm_storage_container" "by_purpose" {
  for_each = local.container_names

  name                  = each.value
  storage_account_id    = azurerm_storage_account.installation.id
  container_access_type = "private" # No public blob access

  depends_on = [azurerm_storage_account.installation]
}

# Data source for storage account properties (DFS endpoint, etc.)
data "azurerm_storage_account" "installation" {
  name                = azurerm_storage_account.installation.name
  resource_group_name = data.azurerm_resource_group.target.name
  depends_on          = [azurerm_storage_account.installation]
}
