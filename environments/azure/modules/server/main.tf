
resource "azurerm_network_interface" "interface" {
  name = "${var.resource_group_name}-vm-${var.index}-interface"
  location = var.resource_group_location
  resource_group_name = var.resource_group_name

  ip_configuration {
    name                          = "internal"
    subnet_id                     = var.subnet_id
    private_ip_address_allocation = "Dynamic"
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