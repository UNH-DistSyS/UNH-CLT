package main

import (
	"flag"

	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/node"
)

var configFile = flag.String("config", "bin/config.json", "Configuration file for client. Defaults to config.json.")

type Console interface {
	StartConsole()
}

func main() {
	flag.Parse()
	id := ids.GetIDFromFlag()
	node := node.NewNode(id)
	node.Run()
}
