output "location" {
  description = "Azure region containing all resources."
  value       = var.location
}

output "resource_group_name" {
  description = "Resource group name."
  value       = data.azurerm_resource_group.target.name
}

output "subscription_id" {
  description = "Azure subscription ID."
  value       = data.azurerm_client_config.current.subscription_id
}

# PostgreSQL Flexible Servers: per-round identifiers, FQDNs, and admin login
output "postgresql_servers" {
  description = "PostgreSQL Flexible Servers by round: id, fqdn, admin_login, sku."
  value = {
    for round in local.v7_round_keys : round => {
      id          = azurerm_postgresql_flexible_server.by_round[round].id
      fqdn        = azurerm_postgresql_flexible_server.by_round[round].fqdn
      name        = azurerm_postgresql_flexible_server.by_round[round].name
      admin_login = azurerm_postgresql_flexible_server.by_round[round].administrator_login
      port        = 5432
      sku_name    = azurerm_postgresql_flexible_server.by_round[round].sku_name
    }
  }
}

# Storage Account
output "storage_account" {
  description = "ADLS Gen2 storage account: name, primary DFS endpoint, container names."
  value = {
    name                 = azurerm_storage_account.installation.name
    primary_dfs_endpoint = azurerm_storage_account.installation.primary_dfs_endpoint
    containers           = local.container_names
  }
}

output "storage_containers" {
  description = "Storage container names."
  value       = local.container_names
}

# Databricks Access Connector
output "access_connector" {
  description = "Databricks Access Connector for Unity Catalog: id, principal_id."
  value = {
    id           = azurerm_databricks_access_connector.uc.id
    principal_id = azurerm_databricks_access_connector.uc.identity[0].principal_id
    name         = azurerm_databricks_access_connector.uc.name
  }
}

# The App's Azure identity: the installer makes the Unity Catalog service credential on it.
output "app_access_connector" {
  description = "The deployed App's Access Connector: id, principal_id, name."
  value = {
    id           = azurerm_databricks_access_connector.app.id
    principal_id = azurerm_databricks_access_connector.app.identity[0].principal_id
    name         = azurerm_databricks_access_connector.app.name
  }
}

# Data Factory
output "data_factory" {
  description = "Azure Data Factory: name, id, principal_id, integration runtime name."
  value = {
    name                     = azurerm_data_factory.installation.name
    id                       = azurerm_data_factory.installation.id
    principal_id             = azurerm_data_factory.installation.identity[0].principal_id
    integration_runtime_name = azurerm_data_factory_integration_runtime_azure.regional.name
  }
}

# Round 5 Runners
output "round5_runners" {
  description = "Round 5 runner VMs: names, resource IDs (Run Command's target), private IPs, principal IDs."
  value = {
    for idx in local.runner_keys : idx => {
      name           = azurerm_linux_virtual_machine.runner[idx].name
      id             = azurerm_linux_virtual_machine.runner[idx].id
      vm_id          = azurerm_linux_virtual_machine.runner[idx].virtual_machine_id
      size           = azurerm_linux_virtual_machine.runner[idx].size
      private_ip     = azurerm_network_interface.runner[idx].private_ip_address
      principal_id   = local.runner_principal_ids[idx]
      admin_username = azurerm_linux_virtual_machine.runner[idx].admin_username
    }
  }
}

output "round5_runner_egress_ip" {
  description = "The runners' one outbound address (their NAT gateway's), which the r5 server admits. Null without runners."
  value       = var.round5_runners ? azurerm_public_ip.round5_nat[0].ip_address : null
}

# Key Vault
output "key_vault" {
  description = "Key Vault for Round 5 runner secrets: name, URI."
  value = {
    name = azurerm_key_vault.round5.name
    uri  = azurerm_key_vault.round5.vault_uri
    id   = azurerm_key_vault.round5.id
  }
}

# Service Bus
output "service_bus" {
  description = "Service Bus namespace and queues for Round 5 control traffic."
  value = {
    namespace_name = azurerm_servicebus_namespace.round5.name
    namespace_id   = azurerm_servicebus_namespace.round5.id
    queues = {
      for name in keys(local.service_bus_queues) :
      name => azurerm_servicebus_queue.round5_runner[name].name
    }
  }
}

# Pending role assignments (when manage_role_assignments = false)
output "pending_role_assignments" {
  description = "Manual role assignments required when manage_role_assignments = false."
  value       = local.pending_role_assignments
}

# Summary
output "installation_summary" {
  description = "Quick reference for the installation: region, storage, runner count, database rounds."
  value = {
    location             = var.location
    storage_account_name = azurerm_storage_account.installation.name
    storage_containers   = length(azurerm_storage_container.by_purpose)
    runner_count         = length(azurerm_linux_virtual_machine.runner)
    database_rounds      = local.v7_round_keys
    managed_by           = "terraform"
  }
}
