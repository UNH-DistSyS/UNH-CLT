resource "aws_network_interface" "interface" {
  subnet_id = var.subnet_id

  security_groups = [
    var.sg_id
  ]
}

resource "aws_eip" "public_ip" {
  network_interface = aws_network_interface.interface.id
  vpc               = true
  public_ipv4_pool  = "amazon"
}

resource "aws_eip_association" "public_ip_for_management_interface" {
  allocation_id        = aws_eip.public_ip.id
  network_interface_id = aws_network_interface.interface.id
}

resource "aws_instance" "app_server" {
  ami           = var.base_image_id
  instance_type = var.instance_type

  # management interface
  network_interface {
    network_interface_id = aws_network_interface.interface.id
    device_index         = 0
  }

  key_name = var.ssh_key_name

  enclave_options {
    enabled = false
  }

  maintenance_options {
    auto_recovery = "disabled"
  }

  root_block_device {
    delete_on_termination = true
    volume_size           = 10
  }
}

resource "aws_ec2_tag" "namespace_tag" {
  resource_id = aws_instance.app_server.id
  key         = "namespace"
  value       = "cloud_latency_tester"
}
