resource "azurerm_network_security_group" "allow_all_sg" {
  name                = "region${var.region_index}-allow_all"
  location            = var.region_name
  resource_group_name = var.resource_group_name

  security_rule {
    name                       = "allow_everything_tcp"
    priority                   = 100
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "*"
    source_address_prefix      = "*"
    destination_address_prefix = "*"
  }

  security_rule {
    name                       = "allow_everything_udp"
    priority                   = 101
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Udp"
    source_port_range          = "*"
    destination_port_range     = "*"
    source_address_prefix      = "*"
    destination_address_prefix = "*"
  }

  tags = {
    environment = "Production"
  }
}

module "network" {
  count = var.region_index == 0 ? 3 : 1

  instance_type = var.instance_type
  network_index = count.index
  region_index = var.region_index
  resource_group_name = var.resource_group_name
  resource_group_location = var.resource_group_location
  ssh_key_path = var.ssh_key_path
  region_name = var.region_name
  sg_id = azurerm_network_security_group.allow_all_sg.id

  source = "../network"
}

output "networks" {
  value = flatten(module.network[*].network)
} 