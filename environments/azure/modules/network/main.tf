resource "azurerm_virtual_network" "network" {
  name                = "cloud-latency-tester-network-${var.resource_group_location}-${var.network_index}"
  address_space       = ["10.0.${(var.region_index * 10) + var.network_index + 1}.0/24"]
  location            = var.resource_group_location
  resource_group_name = var.resource_group_name
}

module "subnet" {
  count = var.network_index == 0 && var.region_index == 0 ? 2 : 1

  network_index = var.network_index
  subnet_index = count.index
  ssh_key_path = var.ssh_key_path
  resource_group_name = var.resource_group_name
  resource_group_location = var.resource_group_location
  region_name = var.region_name
  region_index = var.region_index
  instance_type = var.instance_type
  virtual_network_name = azurerm_virtual_network.network.name

  source = "../subnet"
}
