variable "base_image_id" {
  type        = string
  description = "AMI id to use"
}

variable "ssh_key_name" {
  type        = string
  description = "The name of the SSH key to add to all servers"
}

variable "region_name" {
  type        = string
  description = "The name of the region"
}

variable "region_index" {
  type        = number
  description = "The index of this region"
}

variable "instance_type" {
  type        = string
  description = "What type of instance to use"
  default     = "t3.small"
}
