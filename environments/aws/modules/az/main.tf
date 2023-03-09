
module "subnets" {
  count = var.az_index == 0 && var.region_index == 0 ? 3 : 1

  az_name       = var.az_name
  vpc_id        = var.vpc_id
  base_image_id = var.base_image_id
  sg_id         = var.sg_id
  ssh_key_name  = var.ssh_key_name
  region_index = var.region_index
  az_index      = var.az_index
  subnet_index  = count.index
  instance_type = var.instance_type
  route_table_id = var.route_table_id
  tg_id          = var.tg_id

  source = "../subnet"
}
