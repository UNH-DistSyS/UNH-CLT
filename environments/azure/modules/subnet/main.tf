resource "azurerm_subnet" "subnet" {
  name                 = "region${var.region_index}-net${var.network_index}-subnet${var.subnet_index}"
  address_prefixes     = ["10.0.${(var.region_index * 10) + var.network_index + 1}.${(var.subnet_index + 1) * 8}/29"]
  virtual_network_name = var.virtual_network_name
  resource_group_name  = var.resource_group_name
}

module "server" {
  count = var.subnet_index == 0 && var.network_index == 0 && var.region_index == 0 ? 3 : 1

  index                   = count.index
  instance_type           = var.instance_type
  network_index           = var.network_index
  region_name             = var.region_name
  region_index            = var.region_index
  resource_group_name     = var.resource_group_name
  resource_group_location = var.resource_group_location
  subnet_id               = azurerm_subnet.subnet.id
  ssh_key_path            = var.ssh_key_path
  subnet_index            = var.subnet_index
  sg_id = var.sg_id

  source = "../server"
}
