resource "aws_instance" "app_server" {
  ami           = var.base_image_id
  instance_type = var.instance_type
  subnet_id = var.subnet_id

  vpc_security_group_ids =  [
    var.sg_id
  ]

  associate_public_ip_address = true
  ipv6_address_count = 1

  key_name = var.ssh_key_name

  enclave_options {
    enabled = false
  }

  maintenance_options {
    auto_recovery = "disabled"
  }

  root_block_device {
    delete_on_termination = true
    volume_size           = 15
  }

  tags = {
    Project = "UNHCLT"
  }
}