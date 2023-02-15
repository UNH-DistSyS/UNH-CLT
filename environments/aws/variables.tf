variable "ssh_key_name" {
  type        = string
  description = "The name of the SSH key to add to all servers"
}

variable "instance_type" {
  type        = string
  description = "What type of instance to use"
  default     = "t3.small"
}

variable "region_index" {
  type = number
  description = "Which region number this is"
}

variable "region_name" {
  type = string
  description = "The name of region 0"
}