variable "resource_group_name" {
  type        = string
  description = "The name of this resource group"
}

variable "resource_group_location" {
  type        = string
  description = "The location of the resource group"
}

variable "region_name" {
  type = string
  description = "The name of region 0"
}

variable "region_index" {
  type = number
  description = "What index is this region?"
}

variable "network_index" {
  type        = number
  description = "Which index is this network"
}

variable "subnet_index" {
  type        = number
  description = "What index is this subnet"
}

variable "instance_type" {
  type        = string
  description = "What type of instance to use"
}

variable "ssh_key_path" {
  type = string
  description = "The path to the PUBLIC ssh key you want to use to connect to the server"
}

variable "virtual_network_name" {
  type = string
  description = "The name of the virtual network"
}

variable "sg_id" {
  type = string
  description = "The id of the security group to use"
} 