variable "az_name" {
  type        = string
  description = "The name of this az"
}

variable "vpc_id" {
  type        = string
  description = "The VPC id to use"
}

variable "base_image_id" {
  type        = string
  description = "AMI id to use"
}

variable "sg_id" {
  type        = string
  description = "The security group ID for all ports"
}

variable "ssh_key_name" {
  type        = string
  description = "The name of the SSH key to add to all servers"
}

variable "region_index" {
  type = number
  description = "What index is this region?"
}

variable "az_index" {
  type        = number
  description = "Which index is this az"
}

variable "subnet_index" {
  type        = number
  description = "What index is this subnet"
}

variable "instance_type" {
  type        = string
  description = "What type of instance to use"
  default     = "t3.small"
}
