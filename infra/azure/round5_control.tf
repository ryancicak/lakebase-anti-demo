# Key Vault and Service Bus for Round 5 runner transport
# - Key Vault: holds runner secrets (database credentials, etc.)
# - Service Bus: two queues (one per runner) for ordered control traffic (SQS FIFO equivalent)

resource "azurerm_key_vault" "round5" {
  name                       = local.key_vault_name
  resource_group_name        = data.azurerm_resource_group.target.name
  location                   = var.location
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  purge_protection_enabled   = false # Disposable install; no need for purge protection
  soft_delete_retention_days = 7     # Minimal retention for a disposable install
  # RBAC by default. Access policies when the installer can't assign roles: setting one is a
  # Contributor action (Phase 0).
  rbac_authorization_enabled = var.key_vault_authorization == "rbac"

  tags = merge(local.required_tags_with_lease, {
    "anti-demo-round" = "r5"
  })

  lifecycle {
    ignore_changes = [tags]
  }
}

# Access-policy mode: the installer writes the runner secrets, and each runner reads them.
resource "azurerm_key_vault_access_policy" "installer" {
  count = var.key_vault_authorization == "access_policy" ? 1 : 0

  key_vault_id       = azurerm_key_vault.round5.id
  tenant_id          = data.azurerm_client_config.current.tenant_id
  object_id          = data.azurerm_client_config.current.object_id
  secret_permissions = ["Get", "List", "Set", "Delete", "Purge"]
}

resource "azurerm_key_vault_access_policy" "runner" {
  for_each = var.key_vault_authorization == "access_policy" ? local.runner_keys : toset([])

  key_vault_id       = azurerm_key_vault.round5.id
  tenant_id          = data.azurerm_client_config.current.tenant_id
  object_id          = local.runner_principal_ids[each.key]
  secret_permissions = ["Get", "List"]
}

# Service Bus namespace for control queues
resource "azurerm_servicebus_namespace" "round5" {
  name                = "sb-${local.v7_round_slugs["r5"]}"
  resource_group_name = data.azurerm_resource_group.target.name
  location            = var.location
  sku                 = "Standard"

  # Entra ID only. With local (SAS) auth on, the namespace's RootManageSharedAccessKey lands in
  # Terraform state in plain text, and nothing here uses it: the runners hold Data Sender/Receiver.
  local_auth_enabled = false

  # $10/month charged whether queues exist or not; cost is fixed per namespace

  tags = merge(local.required_tags_with_lease, {
    "anti-demo-round" = "r5"
  })

  lifecycle {
    ignore_changes = [tags]
  }
}

# Two Service Bus queues, one per runner, with session support (ordered delivery)
resource "azurerm_servicebus_queue" "round5_runner" {
  for_each = local.service_bus_queues

  name                = each.value
  namespace_id        = azurerm_servicebus_namespace.round5.id
  requires_session    = true   # Session support for ordered message delivery (FIFO equivalent)
  default_message_ttl = "PT1H" # 1 hour TTL (ISO 8601 duration format)
  # SQS FIFO's other two guarantees, as the AWS edition's queues set them: a control event's ID is
  # its message ID, sent twice and delivered once; and a message is dead-lettered after three
  # deliveries. The lock is Service Bus's maximum (SQS's visibility timeout is 720 s).
  requires_duplicate_detection            = true
  duplicate_detection_history_time_window = "PT10M"
  max_delivery_count                      = 3
  lock_duration                           = "PT5M"
  dead_lettering_on_message_expiration    = true

  depends_on = [azurerm_servicebus_namespace.round5]
}
