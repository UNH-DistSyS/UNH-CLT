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
  shared_config_files      = ["~/.aws/config"]
  shared_credentials_files = ["~/.aws/credentials"]
  region = var.region_name
}

variable "namespace" {
  type    = string
  default = "cloud_latency_tester"
}


data "aws_ami" "ubuntu" {
  most_recent = true

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }

  filter {
    name   = "name"
    values = ["ubuntu/images*22.04*amd64*"]
  }

  owners = ["099720109477"] # Canonical
}

module "region" {
  base_image_id = data.aws_ami.ubuntu.id
  region_name   = var.region_name
  region_index  = var.region_index
  ssh_key_name  = var.ssh_key_name
  instance_type = var.instance_type

  source = "./modules/region"
}
