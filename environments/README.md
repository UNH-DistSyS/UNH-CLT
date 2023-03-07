Environments
============

## General Information

AWS is currently the only complete provider. Azure is a work in progress. 

Makefiles are used to provide default variables to the terraform configuration. Use Makefile variables or edit the file to overwrite these defaults. 

## AWS

* The default instance size is a t4g.nano
  * Overwrite this with "TF_VAR_instance_type=YOUR_INSTANCE_SIZE_HERE"
* Set the TF_VAR_region_#_name variables to change which regions you deploy to. 

### Deployment steps

1. Sign into your AWS account on your local system
2. cd to \<project root\>/environments/aws
3. Run "make apply" to deploy the project
4. When you are done, run "make destroy" with the same variables set to tear down the project. 