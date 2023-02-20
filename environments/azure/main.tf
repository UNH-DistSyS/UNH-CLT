terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "=3.44.1"
    }
  }
}

# Configure the Microsoft Azure Provider
provider "azurerm" {
  features {}
}


module "region-0" {
  region_name   = var.region_0_name
  region_index  = 0
  instance_type = var.instance_type
  ssh_key_path = var.ssh_key_path

  source = "./modules/region"
}

module "region-1" {
  region_name   = var.region_1_name
  region_index  = 1
  instance_type = var.instance_type
  ssh_key_path = var.ssh_key_path

  source = "./modules/region"
}

module "region-2" {
  region_name   = var.region_2_name
  region_index  = 2
  instance_type = var.instance_type
  ssh_key_path = var.ssh_key_path

  source = "./modules/region"
}