
resource "azurerm_public_ip" "public_ip" {
  name                = "${var.resource_group_name}-vm-${var.index}-ip-address"
  resource_group_name = var.resource_group_name
  location            = var.resource_group_location
  allocation_method   = "Dynamic"
  lifecycle {
    create_before_destroy = true
  }

  tags = {
    environment = "Production"
  }
}

resource "azurerm_network_interface" "interface" {
  name = "${var.resource_group_name}-vm-${var.index}-interface"
  location = var.resource_group_location
  resource_group_name = var.resource_group_name

  ip_configuration {
    name                          = "internal"
    subnet_id                     = var.subnet_id
    private_ip_address_allocation = "Dynamic"
    public_ip_address_id = azurerm_public_ip.public_ip.id
  }
}

resource "azurerm_linux_virtual_machine" "app_server" {
  name = "${var.resource_group_name}-vm-${var.index}"
  resource_group_name = var.resource_group_name
  location = var.resource_group_location
  size = var.instance_type
  admin_username = "adminuser"
  network_interface_ids = [
    azurerm_network_interface.interface.id
  ]

  availability_set_id = var.az_set_id

  admin_ssh_key {
    username = "adminuser"
    public_key = file(var.ssh_key_path)
  }

  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "Standard_LRS"
  }

  source_image_reference {
    publisher = "Canonical"
    offer     = "0001-com-ubuntu-server-jammy-daily"
    sku       = "22_04-daily-lts"
    version   = "latest"
  }
}