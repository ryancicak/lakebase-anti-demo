# Firewall rules for PostgreSQL Flexible Servers
# Each server admits the operator /32 and the region's Databricks egress CIDRs (r2-r6).
# Azure firewall rules require explicit start/end IP addresses; convert CIDRs using cidrhost().

# Build a flat map of firewall rules for all servers and all CIDR categories.
# Flatten into a single list: each entry is one firewall rule.
locals {
  # Operator rules: all rounds get operator /32
  operator_rules = flatten([
    for round in local.v7_round_keys : [
      for idx, cidr in local.firewall_rules_by_cidr["operator"].cidrs : {
        round_key        = round
        rule_name        = "AllowOperator"
        cidr             = cidr
        start_ip_address = cidrhost(cidr, 0)
        end_ip_address   = cidrhost(cidr, 0)
      }
    ]
  ])

  # Databricks egress rules: all rounds
  databricks_rules = flatten([
    for round in local.v7_round_keys : [
      for idx, cidr in local.firewall_rules_by_cidr["databricks"].cidrs : {
        round_key        = round
        rule_name        = "AllowDatabricksEgress${idx + 1}"
        cidr             = cidr
        start_ip_address = cidrhost(cidr, 0)
        end_ip_address   = cidrhost(cidr, -1)
      }
    ]
  ])

  # No DataFactory rules: data flows don't leave from the published DataFactory ranges, so
  # admitting them opened the firewall to nothing useful. Rounds 4 and 6 reach their servers
  # through the factory's managed private endpoints (adf.tf).

  # Concatenate all rules and convert to map keyed by deterministic name
  firewall_rules_flat = concat(local.operator_rules, local.databricks_rules)
  firewall_rules_map = {
    for rule in local.firewall_rules_flat :
    "${rule.round_key}:${rule.rule_name}:${replace(rule.start_ip_address, ".", "_")}" => rule
  }
}

resource "azurerm_postgresql_flexible_server_firewall_rule" "all" {
  for_each = local.firewall_rules_map

  name             = each.value.rule_name
  server_id        = azurerm_postgresql_flexible_server.by_round[each.value.round_key].id
  start_ip_address = each.value.start_ip_address
  end_ip_address   = each.value.end_ip_address

  depends_on = [azurerm_postgresql_flexible_server.by_round]
}
