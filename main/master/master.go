package main

import (
	"flag"

	master_provider "github.com/UNH-DistSyS/UNH-CLT/master"

	"github.com/UNH-DistSyS/UNH-CLT/config"
	"github.com/UNH-DistSyS/UNH-CLT/ids"
)

var configFile = flag.String("config", "bin/config.json", "Configuration file for locusdb replica. Defaults to config.json.")
var stop = flag.Bool("stop", false, "Flag to stop testing, defalut false")

func main() {
	flag.Parse()
	id := ids.GetIDFromFlag()
	cfg := config.LoadConfigFromFile(*configFile)
	master := master_provider.NewMaster(cfg, id)
	if *stop {
		master.Stop()
	}
	master.Run()
}
