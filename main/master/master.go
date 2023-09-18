package main

import (
	"flag"
	"math/rand"
	"time"

	"github.com/UNH-DistSyS/UNH-CLT/log"
	"github.com/UNH-DistSyS/UNH-CLT/master_node"

	"github.com/UNH-DistSyS/UNH-CLT/config"
	"github.com/UNH-DistSyS/UNH-CLT/ids"
)

var configFile = flag.String("config", "bin/config.json", "Configuration file for experiment. Defaults to config.json.")
var stop = flag.Bool("stop", false, "Flag to stop testing, defalut false")
var start = flag.Bool("start", false, "Flag to start testing, defalut false")
var close = flag.Bool("close", false, "Flag to close all nodes, defalut false")
var d = flag.Int("test duration", -1, "Flag to setup test duration, default to config duration")
var download_data = flag.Int("download_data", -1, "Flag to download writeup_figures every x min, default to -1(do not download)")

// master -start -config=config.json
func main() {
	flag.Parse()
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	id := ids.NewClientID(uint8(r.Int31n(255)), uint8(r.Int31n(255)))
	cfg := config.LoadConfigFromFile(*configFile)
	master := master_node.NewMaster(cfg, id)
	master.Run()
	if *stop {
		if !master.Stop() {
			log.Errorln("Stop failed!")
		}
	} else if *start {
		if !master.Start(*d) {
			log.Errorln("Start failed!")
		}
	} else if *close {
		if !master.CloseNodes() {
			log.Errorln("Start failed!")
		}
	} else if *download_data > 0 {
		master.Download(*download_data)
	} else {
		if !master.BroadcastConfig() {
			log.Errorln("BroadcastConfig failed!")
		}
	}
}
