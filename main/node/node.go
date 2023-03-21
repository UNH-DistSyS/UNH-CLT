package main

import (
	"flag"
	"strconv"

	"github.com/UNH-DistSyS/UNH-CLT/config"
	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/work_node"
)

var port = flag.Int("port", 1735, "The default port to listen on")

// node -id=1.1 -port=1735
func main() {
	flag.Parse()
	id := ids.GetIDFromFlag()
	cfg := config.MakeDefaultConfig()
	privateAddress := "tcp://127.0.0.1:" + strconv.Itoa(*port)
	publicAddress := "tcp://127.0.0.1:" + strconv.Itoa(*port)
	err := cfg.ClusterMembership.AddNodeAddress(*id, privateAddress, publicAddress)
	if err != nil {
		panic(err)
	}
	node := work_node.NewNode(cfg, *id)
	node.Run()
}
