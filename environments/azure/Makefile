
TF_VAR_region_0_name ?= "East US"
TF_VAR_region_1_name ?= "East US 2"
TF_VAR_region_2_name ?= "West US 3"

init:
	TF_VAR_region_0_name=$(TF_VAR_region_0_name) TF_VAR_region_1_name=$(TF_VAR_region_1_name) TF_VAR_region_2_name=$(TF_VAR_region_2_name) terraform init

plan:
	TF_VAR_region_0_name=$(TF_VAR_region_0_name) TF_VAR_region_1_name=$(TF_VAR_region_1_name) TF_VAR_region_2_name=$(TF_VAR_region_2_name) terraform plan

apply:
	TF_VAR_region_0_name=$(TF_VAR_region_0_name) TF_VAR_region_1_name=$(TF_VAR_region_1_name) TF_VAR_region_2_name=$(TF_VAR_region_2_name) terraform apply

import:
	TF_VAR_region_0_name=$(TF_VAR_region_0_name) TF_VAR_region_1_name=$(TF_VAR_region_1_name) TF_VAR_region_2_name=$(TF_VAR_region_2_name) ./scripts/import_ip_addresses.sh

refresh:
	TF_VAR_region_0_name=$(TF_VAR_region_0_name) TF_VAR_region_1_name=$(TF_VAR_region_1_name) TF_VAR_region_2_name=$(TF_VAR_region_2_name) terraform refresh

destroy:
	TF_VAR_region_0_name=$(TF_VAR_region_0_name) TF_VAR_region_1_name=$(TF_VAR_region_1_name) TF_VAR_region_2_name=$(TF_VAR_region_2_name) terraform destroy