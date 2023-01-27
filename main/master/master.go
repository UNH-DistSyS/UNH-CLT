package main

import (
	"flag"

	"github.com/UNH-DistSyS/UNH-CLT/config"
	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/log"
	"github.com/UNH-DistSyS/UNH-CLT/master"
)

var configFile = flag.String("config", "bin/config.json", "Configuration file for locusdb replica. Defaults to config.json.")

func main() {
	flag.Parse()
	log.Infof("Loading config %s", *configFile)
	cfg := config.LoadConfigFromFile(*configFile)
	id := *ids.GetIDFromFlag()
	// As #Algs are growing, makes it eaiser to switch between different algorithms from config, instead of rebuild each time.
	masterInstance := master.NewMaster(cfg, id)
	masterInstance.Run()
}
