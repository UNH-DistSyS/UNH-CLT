package main

import (
	"flag"

	"github.com/UNH-DistSyS/UNH-CLT/config"
	"github.com/UNH-DistSyS/UNH-CLT/ids"
	master_provider "github.com/UNH-DistSyS/UNH-CLT/master"
)

var configFile = flag.String("config", "bin/config.json", "Configuration file for locusdb replica. Defaults to config.json.")

func main() {
	flag.Parse()
	id := ids.GetIDFromFlag()
	cfg := config.LoadConfigFromFile(*configFile)
	node := master_provider.NewMasterProvider(cfg, id)
	node.Run()
}
