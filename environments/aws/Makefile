
TF_VAR_region_0_name ?= "us-east-1"
TF_VAR_region_1_name ?= "us-east-2"
TF_VAR_region_2_name ?= "us-west-1"

init:
	TF_VAR_region_0_name=$(TF_VAR_region_0_name) TF_VAR_region_1_name=$(TF_VAR_region_1_name) TF_VAR_region_2_name=$(TF_VAR_region_2_name) terraform init

plan:
	TF_VAR_region_0_name=$(TF_VAR_region_0_name) TF_VAR_region_1_name=$(TF_VAR_region_1_name) TF_VAR_region_2_name=$(TF_VAR_region_2_name) terraform plan

apply:
	TF_VAR_region_0_name=$(TF_VAR_region_0_name) TF_VAR_region_1_name=$(TF_VAR_region_1_name) TF_VAR_region_2_name=$(TF_VAR_region_2_name) terraform apply

destroy:
	TF_VAR_region_0_name=$(TF_VAR_region_0_name) TF_VAR_region_1_name=$(TF_VAR_region_1_name) TF_VAR_region_2_name=$(TF_VAR_region_2_name) terraform destroy