package main

import (
	"flag"

	"github.com/UNH-DistSyS/UNH-CLT/config"
	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/work_node"
)

var privateAddress = flag.String("privateAddress", "tcp://127.0.0.1:1735", "The private interface this node will listen on")
var publicAddress = flag.String("publicAddress", "tcp://127.0.0.1:1735", "The public interface this node will listen on")

func main() {
	flag.Parse()
	id := ids.GetIDFromFlag()
	cfg := config.MakeDefaultConfig()
	err := cfg.ClusterMembership.AddNodeAddress(*id, *privateAddress, *publicAddress)
	if err != nil {
		panic(err)
	}
	node := work_node.NewNode(cfg, *id)
	node.Run()
}
