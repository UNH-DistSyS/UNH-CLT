terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "=3.0.0"
    }
  }
}

# Configure the Microsoft Azure Provider
provider "azurerm" {
  features {}
}


module "region" {
  region_name   = var.region_name
  region_index  = var.region_index
  instance_type = var.instance_type
  ssh_key_path = var.ssh_key_path

  source = "./modules/region"
}
