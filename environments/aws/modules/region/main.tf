terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.16"
    }
  }
}

data "aws_ami" "ubuntu" {
  most_recent = true

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }

  filter {
    name   = "name"
    values = ["ubuntu/images*22.04*arm64*"]
  }

  owners = ["099720109477"] # Canonical
}

resource "aws_vpc" "vpc" {
  cidr_block                       = "10.0.0.0/16"
  assign_generated_ipv6_cidr_block = true

  tags = {
    Name    = "cloud_latency_tester_vpc"
    Project = "UNHCLT"
  }
}

resource "aws_ec2_transit_gateway" "tgw" {
  multicast_support = "enable"

  tags = {
    Project = "UNHCLT"
  }
}

resource "aws_internet_gateway" "gw" {
  vpc_id = aws_vpc.vpc.id

  tags = {
    Project = "UNHCLT"
  }
}

resource "aws_route_table" "route_table" {
  vpc_id = aws_vpc.vpc.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.gw.id
  }

  route {
    ipv6_cidr_block = "::/0"
    gateway_id      = aws_internet_gateway.gw.id
  }

  tags = {
    Name    = "Default Route Table"
    Project = "UNHCLT"
  }
}

resource "aws_security_group" "allow_ssh" {
  name   = "allow_ssh-sg"
  vpc_id = aws_vpc.vpc.id

  ingress {
    cidr_blocks = [
      "0.0.0.0/0"
    ]
    ipv6_cidr_blocks = [
      "::/0"
    ]
    from_port = 22
    to_port   = 22
    protocol  = "tcp"
  }

  // Terraform removes the default rule
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

    tags = {
    Project = "UNHCLT"
  }
}

data "aws_availability_zones" "azs" {
  state = "available"

  filter {
    name   = "region-name"
    values = [var.region_name]
  }
}


module "az" {
  count = var.region_index == 0 ? 3 : 1

  az_name        = data.aws_availability_zones.azs.names[count.index]
  vpc_id         = aws_vpc.vpc.id
  route_table_id = aws_route_table.route_table.id
  tg_id          = aws_ec2_transit_gateway.tgw.id
  base_image_id  = data.aws_ami.ubuntu.id
  sg_id          = aws_security_group.allow_ssh.id
  region_index   = var.region_index
  az_index       = count.index
  ssh_key_name   = var.ssh_key_name
  instance_type  = var.instance_type

  source = "../az"
}
