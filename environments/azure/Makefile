
REGION_0_NAME ?= "East US"
REGION_1_NAME ?= "East US 2"
REGION_2_NAME ?= "West US 2"

init:
	terraform init

plan:
	TF_VAR_region_index=0 TF_VAR_region_name=$(REGION_0_NAME) terraform plan
	TF_VAR_region_index=1 TF_VAR_region_name=$(REGION_1_NAME) terraform plan
	TF_VAR_region_index=2 TF_VAR_region_name=$(REGION_2_NAME) terraform plan

apply:
	TF_VAR_region_index=0 TF_VAR_region_name=$(REGION_0_NAME) terraform apply
	# TF_VAR_region_index=1 TF_VAR_region_name=$(REGION_1_NAME) terraform apply
	# TF_VAR_region_index=2 TF_VAR_region_name=$(REGION_2_NAME) terraform apply

destroy:
	TF_VAR_region_index=0 TF_VAR_region_name=$(REGION_0_NAME) terraform destroy
	# TF_VAR_region_index=1 TF_VAR_region_name=$(REGION_1_NAME) terraform destroy
	# TF_VAR_region_index=2 TF_VAR_region_name=$(REGION_2_NAME) terraform destroy