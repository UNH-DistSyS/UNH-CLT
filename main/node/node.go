package main

import (
	"flag"

	"github.com/UNH-DistSyS/UNH-CLT/config"
	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/work_node"
)

var configFile = flag.String("config", "bin/config.json", "Configuration file for client. Defaults to config.json.")

// type Node struct {
// 	netman netwrk.Communicator
// }

func main() {
	flag.Parse()
	id := ids.GetIDFromFlag()
	cfg := config.LoadConfigFromFile(*configFile)
	node := work_node.NewNode(cfg, *id)
	node.Run()
}
