# Round 5 runners: two Standard_D8ls_v7 Linux VMs (Ubuntu 24.04) with managed identities, SSH key
# auth (no password), no public IP and no inbound rules (NSG explicitly allows nothing).
# Control traffic comes via Service Bus queues; the runner's managed identity reads secrets from Key Vault.
#
# Outbound only, through one NAT gateway. A subscription's policy can deny a NIC that carries a public
# IP without an NSG on the NIC itself, which this provider can't attach at creation, so
# the NICs carry none. Run Command needs no inbound path, and the r5 server's firewall admits the NAT
# gateway's one address.

# VNet and subnet for runners
resource "azurerm_virtual_network" "round5" {
  name                = "vnet-${local.v7_round_slugs["r5"]}"
  resource_group_name = data.azurerm_resource_group.target.name
  location            = var.location
  address_space       = ["10.0.0.0/16"]

  tags = merge(local.required_tags_with_lease, {
    "anti-demo-round" = "r5"
  })

  lifecycle {
    ignore_changes = [tags]
  }
}

resource "azurerm_subnet" "round5_runners" {
  name                 = "subnet-runners-${local.v7_round_slugs["r5"]}"
  resource_group_name  = data.azurerm_resource_group.target.name
  virtual_network_name = azurerm_virtual_network.round5.name
  address_prefixes     = ["10.0.1.0/24"]
}

# NSG with NO inbound rules (explicit deny)
resource "azurerm_network_security_group" "round5_runners" {
  name                = "nsg-${local.v7_round_slugs["r5"]}"
  resource_group_name = data.azurerm_resource_group.target.name
  location            = var.location

  # Outbound: allow all (default)
  # Inbound: deny all (no explicit rules, only defaults)

  tags = merge(local.required_tags_with_lease, {
    "anti-demo-round" = "r5"
  })

  lifecycle {
    ignore_changes = [tags]
  }
}

# Attach NSG to subnet
resource "azurerm_subnet_network_security_group_association" "round5_runners" {
  subnet_id                 = azurerm_subnet.round5_runners.id
  network_security_group_id = azurerm_network_security_group.round5_runners.id
}

# The runners' one egress address, zonal beside them.
resource "azurerm_public_ip" "round5_nat" {
  count = var.round5_runners ? 1 : 0

  name                = "pip-${local.v7_round_slugs["r5"]}-nat"
  resource_group_name = data.azurerm_resource_group.target.name
  location            = var.location
  allocation_method   = "Static"
  sku                 = "Standard"
  zones               = var.zone == null ? null : [var.zone]

  tags = merge(local.required_tags_with_lease, {
    "anti-demo-round" = "r5"
  })

  lifecycle {
    ignore_changes = [tags]
  }
}

resource "azurerm_nat_gateway" "round5" {
  count = var.round5_runners ? 1 : 0

  name                    = "natgw-${local.v7_round_slugs["r5"]}"
  resource_group_name     = data.azurerm_resource_group.target.name
  location                = var.location
  sku_name                = "Standard"
  idle_timeout_in_minutes = 10
  zones                   = var.zone == null ? null : [var.zone]

  tags = merge(local.required_tags_with_lease, {
    "anti-demo-round" = "r5"
  })

  lifecycle {
    ignore_changes = [tags]
  }
}

resource "azurerm_nat_gateway_public_ip_association" "round5" {
  count = var.round5_runners ? 1 : 0

  nat_gateway_id       = azurerm_nat_gateway.round5[0].id
  public_ip_address_id = azurerm_public_ip.round5_nat[0].id
}

resource "azurerm_subnet_nat_gateway_association" "round5_runners" {
  count = var.round5_runners ? 1 : 0

  subnet_id      = azurerm_subnet.round5_runners.id
  nat_gateway_id = azurerm_nat_gateway.round5[0].id
}

# Network interfaces for runners: a private address only.
resource "azurerm_network_interface" "runner" {
  for_each = local.runner_keys

  name                = "nic-${local.v7_round_slugs["r5"]}-${each.key}"
  resource_group_name = data.azurerm_resource_group.target.name
  location            = var.location

  ip_configuration {
    name                          = "ipconfig-primary"
    subnet_id                     = azurerm_subnet.round5_runners.id
    private_ip_address_allocation = "Dynamic"
  }

  tags = merge(local.required_tags_with_lease, {
    "anti-demo-round" = "r5"
  })

  lifecycle {
    ignore_changes = [tags]
  }
}

# Linux VM image: Ubuntu 24.04 LTS, latest, Gen2
# The spike (Finding 3) confirmed D8ls_v6 is created in ~35s. westus2 zone 1 later had no v6 left
# (ZonalAllocationFailed, 2026-10-20), so the default is the v7, the same 8 vCPU and 16 GiB.
resource "azurerm_linux_virtual_machine" "runner" {
  for_each = local.runner_keys

  name                = "vm-${local.v7_round_slugs["r5"]}-${each.key}"
  resource_group_name = data.azurerm_resource_group.target.name
  location            = var.location
  size                = var.runner_vm_sku
  zone                = var.zone # beside the r5 server

  admin_username                  = "antidemo_runner"
  disable_password_authentication = true

  network_interface_ids = [azurerm_network_interface.runner[each.key].id]

  # The Dlsv6 sizes take NVMe disks only; the SCSI default fails at create. The S-Z3 runner
  # needed `--disk-controller-type NVMe` for the same reason. The Dlsv7 runners were created with
  # NVMe too (2026-10-20).
  disk_controller_type = "NVMe"

  admin_ssh_key {
    username   = "antidemo_runner"
    public_key = var.runner_ssh_public_key
  }

  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "Standard_LRS"
  }

  source_image_reference {
    publisher = "Canonical"
    offer     = "ubuntu-24_04-lts"
    sku       = "server"
    version   = "latest"
  }

  identity {
    type = "SystemAssigned"
  }

  tags = merge(local.required_tags_with_lease, {
    "anti-demo-round"  = "r5"
    "anti-demo-runner" = each.key
  })

  lifecycle {
    ignore_changes = [tags]
  }

  # Outbound works from first boot: the harness install reaches the package index through it.
  depends_on = [azurerm_subnet_nat_gateway_association.round5_runners]
}

# The r5 server admits the runners' one address, for the Azure lane's pooled path. A resource of
# its own: the address is known only at apply, and `firewall_rules_map`'s keys must be known at plan.
resource "azurerm_postgresql_flexible_server_firewall_rule" "round5_runners" {
  count = var.round5_runners ? 1 : 0

  name             = "AllowRound5Runners"
  server_id        = azurerm_postgresql_flexible_server.by_round["r5"].id
  start_ip_address = azurerm_public_ip.round5_nat[0].ip_address
  end_ip_address   = azurerm_public_ip.round5_nat[0].ip_address
}

# Local map of runner principal IDs for role assignments
# (Direct reference to the identity block in the VM resources)
locals {
  # The runners stand only once Round 5's Azure lane does (var.round5_runners).
  runner_keys = var.round5_runners ? toset(["runner-0", "runner-1"]) : toset([])
  runner_principal_ids = {
    for idx in local.runner_keys :
    idx => azurerm_linux_virtual_machine.runner[idx].identity[0].principal_id
  }
}
