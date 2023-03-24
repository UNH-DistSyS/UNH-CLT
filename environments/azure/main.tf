terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "=3.48.0"
    }
  }
}

# Configure the Microsoft Azure Provider
provider "azurerm" {
  features {}
}

# Create a resource group
resource "azurerm_resource_group" "rg" {
  name     = "Cloud-Latency-Tester"
  location = var.region_0_name
}

module "region-0" {
  region_name   = var.region_0_name
  region_index  = 0
  instance_type = var.instance_type
  ssh_key_path  = var.ssh_key_path

  resource_group_name     = azurerm_resource_group.rg.name
  resource_group_location = azurerm_resource_group.rg.location

  source = "./modules/region"
}

module "region-1" {
  region_name   = var.region_1_name
  region_index  = 1
  instance_type = var.instance_type
  ssh_key_path  = var.ssh_key_path

  resource_group_name     = azurerm_resource_group.rg.name
  resource_group_location = azurerm_resource_group.rg.location

  source = "./modules/region"
}

module "region-2" {
  region_name   = var.region_2_name
  region_index  = 2
  instance_type = var.instance_type
  ssh_key_path  = var.ssh_key_path

  resource_group_name     = azurerm_resource_group.rg.name
  resource_group_location = azurerm_resource_group.rg.location

  source                  = "./modules/region"
}

locals {
  networks = flatten([module.region-0.networks, module.region-1.networks, module.region-2.networks])
  pairs_of_dupes = [for net in local.networks : {
      network1_name = net.name
      network2_name = net.name
      network2_id   = net.id
    }]
  peering_pairs_with_dupes = flatten([
    for pair in setproduct(local.networks, local.networks) : {
      network1_name = pair[0].name
      network2_name = pair[1].name
      network2_id   = pair[1].id
    }
  ])
  peering_pairs = setsubtract(local.peering_pairs_with_dupes, local.pairs_of_dupes)
}

resource "azurerm_virtual_network_peering" "peering" {
  for_each = {
    for pair in local.peering_pairs : "peering-${pair.network1_name}-to-${pair.network2_name}" => pair
  }
  name                         = "peering-${each.value.network1_name}-to-${each.value.network2_name}"
  resource_group_name          = azurerm_resource_group.rg.name
  virtual_network_name         = each.value.network1_name
  remote_virtual_network_id    = each.value.network2_id
  allow_virtual_network_access = true
  allow_forwarded_traffic      = true

  # `allow_gateway_transit` must be set to false for vnet Global Peering
  allow_gateway_transit = false
  depends_on = [
    module.region-0,
    module.region-1,
    module.region-2
  ]
}