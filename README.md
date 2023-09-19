# UNH Cloud Latency Tester

Many distributed systems and algorithms rely on tight timing assumptions on communication in the cluster. 
These assumptions drive failure detectors, leader change mechanisms, or simply help achieve maximum performance by 
helping systems stay in more optimal "(partially-)synchronous" execution paths. Unfortunately, there is not much 
open information about communication timing one may expect in various environments.

Our Cloud Latency Tester (CLT) is a tool to benchmark round-trip latency distributions between nodes in a cluster. 
The tool measures end-to-end latency, as experienced by applications deployed in some servers/VMs. This end-to-end 
latency includes more than just network latency, as the OS kernel, hypervisor, and the application add some latency 
overheads.

![End-to-End Latency](/writeup_figures/end-to-end-latency.png)

The tool is a basic echo-type application running over TCP using the sockets. While this approach is not the most 
sophisticated, it is representative of a typical application or system. Each worker node sends messages of configured 
size to other nodes, each peer echoes received messages, allowing the sender to record the round-trip time between 
itself and all nodes in the cluster. CLT records the data and eventually writes it to a CSV file for processing. 
CLT also provides some tools for the basic processing of data.

## CLT Setup

### Compiling CLT
To build CLT executables, run `make build` command.

### CLT Executables

CLT has three executable files: _master_, _node_, and _processing_. Only the _master_ and _node_ executables are needed to run the experiments.
- _master_ is a utility that starts and stops the testing. The master uses a JSON configuration file that describes the cluster and experimental parameters to connect to all nodes and instructs them to run an experiment.
- _node_ is the actual echo-style application that must be deployed in the desired topology for testing. Nodes also write their raw data locally to disk. Each node has its own unique NodeID expressed in a "zoneId.nodeId" format.

### Configuration File
Configuration JSON file specifies the nodes used in the experiment and the experimental parameters. Below is a sample 
file that expects the CLT to run 3 nodes on the local machine for 20 minutes with 300 messages per second from each 
node, carrying 1024 bytes of payload in each message:

```
{
  "cluster_membership": {
    "private_address": {
       "1.1": "tcp://127.0.0.1:1735",
       "1.2": "tcp://127.0.0.1:1736",
       "1.3": "tcp://127.0.0.1:1737"
    },
    "public_address": {
      "1.1": "tcp://127.0.0.1:1735",
      "1.2": "tcp://127.0.0.1:1736",
      "1.3": "tcp://127.0.0.1:1737"
    }
  },
  "testing_duration_minutes": 20,
  "payload_size":1024,
  "testing_rate_s":300,
  "self_loop":true,
  "csv_prefix": "testing",
  "csv_row_output_limit": 20000000,
  "mem_row_output_limit": 20000000,
  "communication_timeout_ms": 5000
}
```

- The _cluster_membership_ field describes the nodes expected to participate in the experiment. Each node can have a
private and public address. These addresses can be the same, but can also differ, for instance, to test latency on a
connection from a remote region over the public IP interface. As a rule, a node will use the peer's public_address to talk
if the peer has a different "zoneId."

  The addresses are specified in a map if NodeID -> Address, where address is a string specifying a protocol, IP address, and port number in the following format: "tcp://IP:Port". Currently, only TCP protocol is supported.

- The _testing_duration_minutes_ specifies testing duration in minutes. The nodes will automatically stop after that time.
- The _payload_size_ specifies the size of payload in each message, in bytes.
- The _testing_rate_s_ sets the number of communication rounds each node initiates per second. 
- The _self_loop_ parameter specifies whether the nodes talk to themselves.
- The _csv_prefix_ is the file name prefix for raw CSV data.
- The _csv_row_output_limit_ sets maximum size of CSV.
- The _mem_row_output_limit_ sets maximum number of raw data entries kept in memory before writing to CSV. It is possible to store many hours worth of data in memory before writing out to disk. 
- The _communication_timeout_ms_ is the upper limit on round-trip latency the tool expects. Essentially, this is a timeout for rounds taking too long.  

### Starting Nodes
To start a node, place the executable to a desired machine, and start it with the following command:
 - `node -id=<NodeID> -port=<portNum>`

   The NodeId must be a valid NodeID from a testing configuration file eventually supplied to _master_. Similarly, the port number must be the same port number as described in the address string in the configuration file.

### Starting the Experiment
To start the experiment, use the master utility with the following command:
- `master -start -config=<configFile.json>`

  The _master_ utility will connect to all nodes specified in the config file, deliver the configuration parameters, such as communication rate and payload size, and struct the nodes to start the experiment. 

### Processing the Data

Coming soon

## Cloud Latency Report (March 2023)
In March 2023, we deployed CLT in three public clouds - Azure, AWS, and GCP. The deployment topology is shown below: 
![CLT Deployement in the Cloud](/writeup_figures/deployement.png)

We focused on testing latency between nodes/processes in the same region, although we performed some measurements 
across WAN. Nodes 1.1, 1.2, and 1.3 were deployed in the same subnet of the same availability zone. Node 1.4 is in the 
same AZ as nodes 1.1 - 1.3, but different subnet. Nodes 1.5 and 1.6 are in different AZs of the same region. We also 
used nodes 2.1 and 3.1 in different regions.

In each cloud we used smaller 2 vCPU VMs with 8 GB of RAM. While these VMs are on a smaller side, they represent more
popular VM types in each cloud. We deployed CLT on top of Ubuntu 22.04 LTS. 

Figure below shows the summary over a 6-hour run conducted on a weekday:
![Same-region latency data](/writeup_figures/same-region-summary.png)

A more detailed view at latency distribution (top) and latency between individual nodes (bottom) in the same subnet of the AZ (nodes 1.1 - 1.3):
![Same subnet latency data](/writeup_figures/same-subnet-cloud-data.png)

Latency distribution across AZs of the same region:
![Latency between nodes in different AZs of a region](/writeup_figures/cross-az.png)

Latency for different payload sizes:
![Latency for different payload sizes](/writeup_figures/payload.png)

Latency for quorums:
![Latency for majority quorum](/writeup_figures/quorum.png)

Cross-region latency:
![Cross-region latency](/writeup_figures/cross-region.png)

For more data and details, please see the full report (coming soon)

## Testing Environment

Terraform scripts for each supported cloud provider can be found at "./environments/$PROVIDER/"

## Specialized Tools

Specialized tools that do not fit into the main purpose of the project are stored under the "tools" directory. Each will contain it's own README explaining what it's purpose is.