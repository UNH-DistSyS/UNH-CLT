resource "aws_subnet" "subnet" {
  vpc_id                                         = var.vpc_id
  availability_zone                              = var.az_name
  cidr_block                                     = "10.0.${(var.region_index * 10) + var.az_index}.${var.subnet_index * 2}/31"
  map_public_ip_on_launch                        = true
  assign_ipv6_address_on_creation                = true
  enable_resource_name_dns_a_record_on_launch    = true
  enable_resource_name_dns_aaaa_record_on_launch = true

  tags = {
    Name = "net${var.subnet_index}"
  }
}

module "server" {
  count = var.subnet_index == 0 && var.az_index == 0 && var.region_index == 0 ? 3 : var.az_index == 2 && var.region_index == 0 ? 2 : 1

  availability_zone_name = var.az_name
  base_image_id          = var.base_image_id
  sg_id                  = var.sg_id
  subnet_id              = aws_subnet.subnet.id
  ssh_key_name           = var.ssh_key_name
  index                  = count.index
  instance_type = var.instance_type

  source = "../server"
}
