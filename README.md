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

## Cluster Setup

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
  "communication_timeout_ms": 5000,
  "chan_buffer_size": 1024,
  "op_dispatch_concurrency": 255
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

## Testing Environment

Terraform scripts for each supported cloud provider can be found at "./environments/$PROVIDER/"

## Specialized Tools

Specialized tools that do not fit into the main purpose of the project are stored under the "tools" directory. Each will contain it's own README explaining what it's purpose is.