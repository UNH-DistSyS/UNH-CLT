resource "aws_default_vpc" "default" {
  tags = {
    Name = "cloud_latency_tester_vpc"
  }
}

resource "aws_security_group" "allow_ssh" {
  name   = "allow_ssh-sg"
  vpc_id = aws_default_vpc.default.id
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

  az_name       = data.aws_availability_zones.azs.names[count.index]
  vpc_id        = aws_default_vpc.default.id
  base_image_id = var.base_image_id
  sg_id         = aws_security_group.allow_ssh.id
  region_index  = var.region_index
  az_index      = count.index
  ssh_key_name  = var.ssh_key_name
  instance_type = var.instance_type

  source = "../az"
}