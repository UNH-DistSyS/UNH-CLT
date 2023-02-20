data "aws_vpc" "vpc" {
  id = var.vpc_id
}


resource "aws_subnet" "subnet" {
  vpc_id                                         = var.vpc_id
  availability_zone                              = var.az_name
  cidr_block                                     = cidrsubnet(cidrsubnet(cidrsubnet(data.aws_vpc.vpc.cidr_block, 4, var.region_index), 4, var.az_index), 4, var.subnet_index)
  ipv6_cidr_block                                = cidrsubnet(cidrsubnet(cidrsubnet(data.aws_vpc.vpc.ipv6_cidr_block, 4, var.region_index), 2, var.az_index), 2, var.subnet_index)
  map_public_ip_on_launch                        = true
  assign_ipv6_address_on_creation                = true
  enable_resource_name_dns_a_record_on_launch    = true

  tags = {
    Name = "net${var.subnet_index}"
    Project = "UNHCLT"
  }
}

resource "aws_route_table_association" "management_route_table_association" {
  subnet_id      = aws_subnet.subnet.id
  route_table_id = var.route_table_id
}

# resource "aws_ec2_transit_gateway_vpc_attachment" "tgw_vpc_attach" {
#   subnet_ids         = [aws_subnet.subnet.id]
#   transit_gateway_id = var.tg_id
#   vpc_id             = var.vpc_id
# }

module "server" {
  count = var.subnet_index == 0 && var.az_index == 0 && var.region_index == 0 ? 3 : var.az_index == 1 && var.region_index == 0 ? 2 : 1

  availability_zone_name = var.az_name
  base_image_id          = var.base_image_id
  sg_id                  = var.sg_id
  subnet_id              = aws_subnet.subnet.id
  ssh_key_name           = var.ssh_key_name
  index                  = count.index
  instance_type          = var.instance_type

  source = "../server"
}
