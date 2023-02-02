package main

import (
	"flag"

	"github.com/UNH-DistSyS/UNH-CLT/config"
	"github.com/UNH-DistSyS/UNH-CLT/ids"
	node_provider "github.com/UNH-DistSyS/UNH-CLT/node"
)

var configFile = flag.String("config", "bin/config.json", "Configuration file for client. Defaults to config.json.")

// type Node struct {
// 	netman netwrk.Communicator
// }

func main() {
	flag.Parse()
	id := ids.GetIDFromFlag()
	cfg := config.LoadConfigFromFile(*configFile)
	node := node_provider.NewNode(cfg, id)
	node.Run()
}
