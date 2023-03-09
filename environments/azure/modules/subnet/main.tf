resource "azurerm_subnet" "subnet" {
  name          = "cloud-latency-tester-subnet-${var.resource_group_location}-${var.network_index}-${var.subnet_index}"
  address_prefixes = ["10.0.${(var.region_index * 10) + var.network_index + 1}.${(var.subnet_index + 1) * 8}/29"]
  virtual_network_name = var.virtual_network_name
  resource_group_name = var.resource_group_name
}


resource "azurerm_availability_set" "az" {
  name                = "cloud-latency-tester-network-${var.resource_group_location}-az-${var.network_index}"
  location            = var.resource_group_location
  resource_group_name = var.resource_group_name
}

module "server" {
  count = var.subnet_index == 0 && var.network_index == 0 && var.region_index == 0 ? 3 : var.network_index == 1 && var.region_index == 0 ? 2 : 1

  index                   = count.index
  instance_type           = var.instance_type
  resource_group_name     = var.resource_group_name
  resource_group_location = var.resource_group_location
  subnet_id = azurerm_subnet.subnet.id
  ssh_key_path = var.ssh_key_path
  az_set_id = azurerm_availability_set.az.id

  source = "../server"
}
