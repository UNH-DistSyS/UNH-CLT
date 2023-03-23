variable "resource_group_name" {
  type        = string
  description = "The name of the resource group to use"
}

variable "resource_group_location" {
  type        = string
  description = "The location to store put this server"
}

variable "index" {
  type        = number
  description = "The index of this server"
}

variable "region_name" {
  type        = string
  description = "The name of the region"
}

variable "region_index" {
  type = number
  description = "What index is this region?"
}
variable "instance_type" {
  type        = string
  description = "What type of instance to use"
}

variable "network_index" {
  type        = number
  description = "The index of the network this is in (used for determining AZ)"
}

variable "subnet_id" {
  type        = string
  description = "The subnet to bind these servers to"
}

variable "subnet_index" {
  type        = number
  description = "What index is this subnet"
}

variable "ssh_key_path" {
  type        = string
  description = "The path to the PUBLIC ssh key you want to use to connect to the server"
}

variable "sg_id" {
  type = string
  description = "The id of the security group to use"
} 