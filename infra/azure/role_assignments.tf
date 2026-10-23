# RBAC role assignments for Azure service principals to access resources.
# When var.manage_role_assignments = true, Terraform creates the assignments.
# When false, the assignments are output for manual creation by an administrator
# (e.g., in a subscription where the installer principal has Contributor but not User Access Administrator).

# Role assignment: ADF identity -> Storage Blob Data Contributor on storage account
resource "azurerm_role_assignment" "adf_storage_contributor" {
  count = var.manage_role_assignments ? 1 : 0

  scope                = azurerm_storage_account.installation.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_data_factory.installation.identity[0].principal_id
}

# Role assignment: Access Connector identity -> Storage Blob Data Contributor on storage account
resource "azurerm_role_assignment" "ac_storage_contributor" {
  count = var.manage_role_assignments ? 1 : 0

  scope                = azurerm_storage_account.installation.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.uc.identity[0].principal_id
}

# Role assignment: the App's identity -> Contributor on the installation's resource group. The
# App restores and deletes Rounds 2 and 3's copies and writes their firewall rules, turns Round 5's
# PgBouncer on and off, starts and cancels Rounds 4 and 6's data flows, and moves the lease tags.
# The group holds the installation alone; Phase 0 ran every one of those calls under this role.
resource "azurerm_role_assignment" "app_group_contributor" {
  count = var.manage_role_assignments ? 1 : 0

  scope                = data.azurerm_resource_group.target.id
  role_definition_name = "Contributor"
  principal_id         = azurerm_databricks_access_connector.app.identity[0].principal_id
}

locals {
  # In access-policy mode the vault needs no role assignments at all (round5_control.tf).
  key_vault_rbac = var.key_vault_authorization == "rbac"
}

# Role assignments (RBAC vault): each runner identity -> Key Vault Secrets User on Key Vault
resource "azurerm_role_assignment" "runner_keyvault_secrets_user" {
  for_each = var.manage_role_assignments && local.key_vault_rbac ? local.runner_keys : toset([])

  scope                = azurerm_key_vault.round5.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = local.runner_principal_ids[each.key]
}

# Role assignment (RBAC vault): the installer -> Key Vault Secrets Officer, to write the runner
# secrets. Contributor manages the vault but grants no access to its secrets in RBAC mode.
resource "azurerm_role_assignment" "installer_keyvault_secrets_officer" {
  count = var.manage_role_assignments && local.key_vault_rbac ? 1 : 0

  scope                = azurerm_key_vault.round5.id
  role_definition_name = "Key Vault Secrets Officer"
  principal_id         = data.azurerm_client_config.current.object_id
}

# Role assignments: each runner identity -> Azure Service Bus Data Receiver/Sender on its queue
resource "azurerm_role_assignment" "runner_servicebus_receiver" {
  for_each = var.manage_role_assignments && var.round5_runners ? local.service_bus_queues : {}

  scope                = "${azurerm_servicebus_namespace.round5.id}/queues/${each.value}"
  role_definition_name = "Azure Service Bus Data Receiver"
  principal_id         = local.runner_principal_ids[replace(each.key, "r5-runner-", "runner-")]
}

resource "azurerm_role_assignment" "runner_servicebus_sender" {
  for_each = var.manage_role_assignments && var.round5_runners ? local.service_bus_queues : {}

  scope                = "${azurerm_servicebus_namespace.round5.id}/queues/${each.value}"
  role_definition_name = "Azure Service Bus Data Sender"
  principal_id         = local.runner_principal_ids[replace(each.key, "r5-runner-", "runner-")]
}

# Role assignments: whoever sends the runners their control events -> Azure Service Bus Data Sender
# on each runner's queue. The App sends them during a bout, and the installer while it proves the
# transport at seal time. The namespace has local (SAS) auth off, so there is no key to fall back
# on, and Contributor carries no data-plane right.
locals {
  round5_control_senders = var.round5_runners ? {
    app       = azurerm_databricks_access_connector.app.identity[0].principal_id
    installer = data.azurerm_client_config.current.object_id
  } : {}
}

resource "azurerm_role_assignment" "control_servicebus_sender" {
  for_each = var.manage_role_assignments ? {
    for pair in setproduct(keys(local.round5_control_senders), keys(local.service_bus_queues)) :
    "${pair[0]}:${pair[1]}" => { sender = pair[0], queue = local.service_bus_queues[pair[1]] }
  } : {}

  scope                = "${azurerm_servicebus_namespace.round5.id}/queues/${each.value.queue}"
  role_definition_name = "Azure Service Bus Data Sender"
  principal_id         = local.round5_control_senders[each.value.sender]
}

# Pending role assignments (output only when manage_role_assignments = false)
# An administrator creates these manually using `az role assignment create`.
locals {
  # Every assignment the resources above would have made, so an operator without User Access
  # Administrator can hand the exact list to someone who has it.
  pending_role_assignments = var.manage_role_assignments ? [] : concat(
    [
      {
        principal_id   = azurerm_data_factory.installation.identity[0].principal_id
        principal_name = "DataFactory(${azurerm_data_factory.installation.name})"
        role           = "Storage Blob Data Contributor"
        scope          = azurerm_storage_account.installation.id
        resource       = "storage account"
      },
      {
        principal_id   = azurerm_databricks_access_connector.uc.identity[0].principal_id
        principal_name = "AccessConnector(${azurerm_databricks_access_connector.uc.name})"
        role           = "Storage Blob Data Contributor"
        scope          = azurerm_storage_account.installation.id
        resource       = "storage account"
      },
      {
        principal_id   = azurerm_databricks_access_connector.app.identity[0].principal_id
        principal_name = "AccessConnector(${azurerm_databricks_access_connector.app.name})"
        role           = "Contributor"
        scope          = data.azurerm_resource_group.target.id
        resource       = "resource group"
      },
    ],
    local.key_vault_rbac ? [
      for runner in local.runner_keys : {
        principal_id   = local.runner_principal_ids[runner]
        principal_name = "RunnerVM(${azurerm_linux_virtual_machine.runner[runner].name})"
        role           = "Key Vault Secrets User"
        scope          = azurerm_key_vault.round5.id
        resource       = "key vault"
      }
    ] : [],
    local.key_vault_rbac ? [
      {
        principal_id   = data.azurerm_client_config.current.object_id
        principal_name = "Installer(${data.azurerm_client_config.current.object_id})"
        role           = "Key Vault Secrets Officer"
        scope          = azurerm_key_vault.round5.id
        resource       = "key vault"
      }
    ] : [],
    flatten([
      for key, queue in(var.round5_runners ? local.service_bus_queues : {}) : [
        for role in ["Azure Service Bus Data Receiver", "Azure Service Bus Data Sender"] : {
          principal_id   = local.runner_principal_ids[replace(key, "r5-runner-", "runner-")]
          principal_name = "RunnerVM(${azurerm_linux_virtual_machine.runner[replace(key, "r5-runner-", "runner-")].name})"
          role           = role
          scope          = "${azurerm_servicebus_namespace.round5.id}/queues/${queue}"
          resource       = "service bus queue"
        }
      ]
    ]),
    [
      for pair in setproduct(keys(local.round5_control_senders), values(local.service_bus_queues)) : {
        principal_id   = local.round5_control_senders[pair[0]]
        principal_name = pair[0] == "app" ? "AccessConnector(${azurerm_databricks_access_connector.app.name})" : "Installer(${data.azurerm_client_config.current.object_id})"
        role           = "Azure Service Bus Data Sender"
        scope          = "${azurerm_servicebus_namespace.round5.id}/queues/${pair[1]}"
        resource       = "service bus queue"
      }
    ],
  )
}
