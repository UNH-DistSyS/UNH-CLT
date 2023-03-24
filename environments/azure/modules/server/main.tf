resource "azurerm_network_interface" "interface" {
  name                = "region${var.region_index}-az${tostring(var.network_index)}-net${var.subnet_index}-vm${var.index}-interface"
  location            = var.region_name
  resource_group_name = var.resource_group_name

  ip_configuration {
    name                          = "internal"
    subnet_id                     = var.subnet_id
    private_ip_address_allocation = "Dynamic"
    # public_ip_address_id          = lookup(var.public_ips, "region${var.region_index}-az${tostring(var.network_index)}-vm${var.index}-ip-address")
  }
}

resource "azurerm_network_interface_security_group_association" "sg_association" {
  network_interface_id      = azurerm_network_interface.interface.id
  network_security_group_id = var.sg_id
}

resource "azurerm_linux_virtual_machine" "app_server" {
  name                = "region${var.region_index}-az${tostring(var.network_index)}-net${var.subnet_index}-vm${var.index}"
  resource_group_name = var.resource_group_name
  location            = var.region_name
  size                = var.instance_type
  admin_username      = "adminuser"
  network_interface_ids = [
    azurerm_network_interface.interface.id
  ]

  zone = var.network_index + 1

  admin_ssh_key {
    username   = "adminuser"
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
