package main

import (
	"flag"

	"github.com/UNH-DistSyS/UNH-CLT/log"
	master_provider "github.com/UNH-DistSyS/UNH-CLT/master"

	"github.com/UNH-DistSyS/UNH-CLT/config"
	"github.com/UNH-DistSyS/UNH-CLT/ids"
)

var configFile = flag.String("config", "bin/config.json", "Configuration file for locusdb replica. Defaults to config.json.")
var stop = flag.Bool("stop", false, "Flag to stop testing, defalut false")
var start = flag.Bool("start", false, "Flag to start testing, defalut false")
var d = flag.Int("test duration", -1, "Flag to setup test duration, default to infinity")

func main() {
	flag.Parse()
	id := ids.GetIDFromFlag()
	cfg := config.LoadConfigFromFile(*configFile)
	master := master_provider.NewMaster(cfg, id)
	if *stop {
		if !master.Stop() {
			log.Errorln("Stop failed!")
		}
	} else if *start {
		if !master.Start(*d) {
			log.Errorln("Start failed!")
		}
	} else {
		if !master.BroadcastConfig() {
			log.Errorln("BroadcastConfig failed!")
		}
	}
	master.Run()
}
