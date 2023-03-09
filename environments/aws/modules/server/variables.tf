variable "availability_zone_name" {
  type        = string
  description = "The name of the availability zone to use"
}

variable "base_image_id" {
  type        = string
  description = "AMI id to use"
}

variable "sg_id" {
  type        = string
  description = "The security group ID for the management ports"
}

variable "subnet_id" {
  type        = string
  description = "The subnet to use for management"
}

variable "index" {
  type        = number
  description = "The index of this server"
}

variable "ssh_key_name" {
  type        = string
  description = "The name of the SSH key to add to all servers"
}

variable "instance_type" {
  type        = string
  description = "What type of instance to use"
  default     = "t3.small"
}