terraform {

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.16"
    }
  }

  required_version = ">= 1.2.0"
}

provider "aws" {
  alias = "region-0"

  shared_config_files      = ["~/.aws/config"]
  shared_credentials_files = ["~/.aws/credentials"]
  region                   = var.region_0_name
}

provider "aws" {
  alias = "region-1"

  shared_config_files      = ["~/.aws/config"]
  shared_credentials_files = ["~/.aws/credentials"]
  region                   = var.region_1_name
}

provider "aws" {
  alias = "region-2"

  shared_config_files      = ["~/.aws/config"]
  shared_credentials_files = ["~/.aws/credentials"]
  region                   = var.region_2_name
}

variable "namespace" {
  type    = string
  default = "cloud_latency_tester"
}

module "region-0" {
  region_name   = var.region_0_name
  region_index  = 0
  ssh_key_name  = var.ssh_key_name
  instance_type = var.instance_type

  providers = {
    aws = aws.region-0
  }

  source = "./modules/region"
}

module "region-1" {
  region_name   = var.region_1_name
  region_index  = 1
  ssh_key_name  = var.ssh_key_name
  instance_type = var.instance_type

  providers = {
    aws = aws.region-1
  }

  source = "./modules/region"
}

module "region-2" {
  region_name   = var.region_2_name
  region_index  = 2
  ssh_key_name  = var.ssh_key_name
  instance_type = var.instance_type

  providers = {
    aws = aws.region-2
  }

  source = "./modules/region"
}
