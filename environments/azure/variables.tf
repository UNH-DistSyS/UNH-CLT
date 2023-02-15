variable "ssh_key_path" {
  type = string
  description = "The path to the PUBLIC ssh key you want to use to connect to the server"
}

variable "instance_type" {
  type        = string
  description = "What type of instance to use"
  default = "Standard_B1ls"
}

variable "region_index" {
  type = number
  description = "Which region number this is"
}

variable "region_name" {
  type = string
  description = "The name of the region"
}