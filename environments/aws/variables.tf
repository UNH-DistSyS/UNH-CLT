variable "ssh_key_name" {
  type        = string
  description = "The name of the SSH key to add to all servers"
}

variable "instance_type" {
  type        = string
  description = "What type of instance to use"
  default     = "t3.nano"
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