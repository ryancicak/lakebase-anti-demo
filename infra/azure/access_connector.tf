# Databricks Access Connector for Azure: provides managed identity for Unity Catalog
# to read/write the storage account.

resource "azurerm_databricks_access_connector" "uc" {
  name                = "ac-${local.v7_round_slugs["r4"]}"
  resource_group_name = data.azurerm_resource_group.target.name
  location            = var.location
  identity {
    type = "SystemAssigned"
  }

  tags = merge(local.required_tags_with_lease, {
    "anti-demo-purpose" = "unity-catalog-identity"
  })

  lifecycle {
    ignore_changes = [tags]
  }
}

# The deployed App's Azure identity, kept apart from Unity Catalog's above. The App
# reaches ARM through a Unity Catalog service credential on this connector, which mints it a
# short-lived token for this identity: no app registration and no client secret anywhere. Its only
# role is Contributor on this resource group (role_assignments.tf).
resource "azurerm_databricks_access_connector" "app" {
  name                = "ac-${local.app_slug}"
  resource_group_name = data.azurerm_resource_group.target.name
  location            = var.location
  identity {
    type = "SystemAssigned"
  }

  tags = merge(local.required_tags_with_lease, {
    "anti-demo-purpose" = "app-azure-identity"
  })

  lifecycle {
    ignore_changes = [tags]
  }
}
