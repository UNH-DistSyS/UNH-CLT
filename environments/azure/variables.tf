variable "ssh_key_path" {
  type        = string
  description = "The path to the PUBLIC ssh key you want to use to connect to the server"
}

variable "instance_type" {
  type        = string
  description = "What type of instance to use"
  default     = "Standard_B1ls"
}

variable "region_0_name" {
  type        = string
  description = "The name of region 0"
}

variable "region_1_name" {
  type        = string
  description = "The name of region 1"
}

variable "region_2_name" {
  type        = string
  description = "The name of region 2"
}
