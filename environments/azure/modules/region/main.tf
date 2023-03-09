# Create a resource group
resource "azurerm_resource_group" "rg" {
  name     = "Cloud-Latency-Tester-${var.region_index}"
  location = var.region_name
}


module "network" {
  count = var.region_index == 0 ? 3 : 1

  instance_type = var.instance_type
  network_index = count.index
  region_index = var.region_index
  resource_group_name = azurerm_resource_group.rg.name
  resource_group_location = azurerm_resource_group.rg.location
  ssh_key_path = var.ssh_key_path
  region_name = var.region_name

  source = "../network"
}