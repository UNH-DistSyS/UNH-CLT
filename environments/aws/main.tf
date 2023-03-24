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

data "aws_caller_identity" "region-0" {
  provider = aws.region-0
}

data "aws_caller_identity" "region-1" {
  provider = aws.region-1
}

data "aws_caller_identity" "region-2" {
  provider = aws.region-2
}

resource "aws_vpc_peering_connection" "peering-0-1" {
  vpc_id      = module.region-0.vpc_id
  peer_vpc_id = module.region-1.vpc_id
  auto_accept = false
  provider    = aws.region-0
  peer_region = var.region_1_name
}

resource "aws_vpc_peering_connection_accepter" "peering-0-1-acceptor" {
  provider                  = aws.region-1
  vpc_peering_connection_id = aws_vpc_peering_connection.peering-0-1.id
  auto_accept               = true
}

resource "aws_route" "peering-0-1-route" {
  provider = aws.region-0
  destination_cidr_block = module.region-1.vpc.cidr_block
  vpc_peering_connection_id = aws_vpc_peering_connection.peering-0-1.id
  route_table_id = module.region-0.route_table_id
}

resource "aws_route" "peering-1-0-route" {
  provider = aws.region-1
  destination_cidr_block = module.region-0.vpc.cidr_block
  vpc_peering_connection_id = aws_vpc_peering_connection.peering-0-1.id
  route_table_id = module.region-1.route_table_id
}

resource "aws_vpc_peering_connection" "peering-0-2" {
  vpc_id      = module.region-0.vpc_id
  peer_vpc_id = module.region-2.vpc_id
  auto_accept = false
  provider    = aws.region-0
  peer_region = var.region_2_name
}

resource "aws_vpc_peering_connection_accepter" "peering-0-2-acceptor" {
  provider                  = aws.region-2
  vpc_peering_connection_id = aws_vpc_peering_connection.peering-0-2.id
  auto_accept               = true
}

resource "aws_route" "peering-0-2-route" {
  provider = aws.region-0
  destination_cidr_block = module.region-2.vpc.cidr_block
  vpc_peering_connection_id = aws_vpc_peering_connection.peering-0-2.id
  route_table_id = module.region-0.route_table_id
}

resource "aws_route" "peering-2-0-route" {
  provider = aws.region-2
  destination_cidr_block = module.region-0.vpc.cidr_block
  vpc_peering_connection_id = aws_vpc_peering_connection.peering-0-2.id
  route_table_id = module.region-2.route_table_id
}

resource "aws_vpc_peering_connection" "peering-1-2" {
  vpc_id      = module.region-1.vpc_id
  peer_vpc_id = module.region-2.vpc_id
  auto_accept = false
  provider    = aws.region-1
  peer_region = var.region_2_name
}

resource "aws_vpc_peering_connection_accepter" "peering-1-2-acceptor" {
  provider                  = aws.region-2
  vpc_peering_connection_id = aws_vpc_peering_connection.peering-1-2.id
  auto_accept               = true
}

resource "aws_route" "peering-1-2-route" {
  provider = aws.region-1
  destination_cidr_block = module.region-2.vpc.cidr_block
  vpc_peering_connection_id = aws_vpc_peering_connection.peering-1-2.id
  route_table_id = module.region-1.route_table_id
}

resource "aws_route" "peering-2-1-route" {
  provider = aws.region-2
  destination_cidr_block = module.region-1.vpc.cidr_block
  vpc_peering_connection_id = aws_vpc_peering_connection.peering-1-2.id
  route_table_id = module.region-2.route_table_id
}