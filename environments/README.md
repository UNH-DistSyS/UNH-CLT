Environments
============

## General Information

AWS is currently the only complete provider. Azure is a work in progress. For the paper associated with this project, Google Cloud Platform was deployed manually. 

Makefiles are used to provide default variables to the terraform configuration. Use Makefile variables or edit the file to overwrite these defaults. 

Whenever possible, Intel Skylake CPUs were selected to help minimize differences across clouds. 

## General Topology

Every cloud is deployed roughly in the same configuration. 3 Regions, with the first one, Region 0, having 3 AZs (or the logical equivalent) and the others having one. All AZs except for AZ 0 in Region 0 have a single node in them. AZ 0 in Region 0 contains two logical networks. Network 0 has 3 nodes, showing a best case for a consensus algorithm that chooses to sacrifice some measure of fault tolerance. Network 1 in AZ 0 in Region 0 has a single node, which is used to help test the overhead of traversing the mechanism used for separating logical networks. While less important in smaller deployments which all fit into a typical IPv4/24 subnet, sufficiently large deployments on some clouds can be forced to use multiple networks. All internal IP addresses are globally unique, and to the extent of our knowledge no Network Address Translation should be necessary or active. AZs 1 and 2 in Region 0 each have one node, and depending on the cloud provider they may or may not be in the same subnet as Region 0 AZ 0 Network 0. Finally, Regions 1 and 2 each have one node. 

### Assumptions of Physical Topology

It is our assumption that, from a networking perspective, the following is roughly accurate. A separate network is a separate virtualized L2 Ethernet network, meaning that IP addresses are required to address between them. An AZ is either an entire datacenter or a distinct section of a single datacenter with redundant power, cooling and networking. A Region is one or more datacenters in a distinct geographic area, likely with provider-owned backhaul networks between the datacenters if there are multiple datacenters. It is from these assumptions that the logical topology was derived, as we intentionally chose ways to divide the nodes that we thought could introduce additional latency. 

## AWS

* The default instance size is a t4g.nano, but we used m5.large for testing
  * Overwrite this with "TF_VAR_instance_type=YOUR_INSTANCE_SIZE_HERE"
* Set the TF_VAR_region_#_name variables to change which regions you deploy to. 

### Deployment steps

1. Sign into your AWS account on your local system
2. cd to \<project root\>/environments/aws
3. Run "make apply" to deploy the project
4. When you are done, run "make destroy" with the same variables set to tear down the project. 

### Topology 

AWS requires a separate VPC for each region, and each AZ has at least one network. AZs with a single node have only one network. 

## Google Cloud Platform (GCP)

* The instance used was e2-micro

### Deployment Steps

Deployment for GCP was done manually, matching the topology described below as closely as possible

### Topology

GCP allows one VPC to be multi-region, so only a single one was used. Networks, called subnets in GCP, are also at a region granularity, instead of AZ. This means that in Region 0, the nodes in AZ 1 and 2 are both in the same subnet as the nodes in Region 0, AZ 0, Network 0 as described in our general topology.