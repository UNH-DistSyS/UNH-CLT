variable "resource_group_name" {
  type = string
  description = "The name of the resource group to use"
}

variable "resource_group_location" {
  type = string
  description = "The location to store put this server"
}

variable "index" {
  type        = number
  description = "The index of this server"
}

variable "instance_type" {
  type        = string
  description = "What type of instance to use"
}

variable "subnet_id" {
  type = string
  description = "The subnet to bind these servers to"
}

variable "ssh_key_path" {
  type = string
  description = "The path to the PUBLIC ssh key you want to use to connect to the server"
}