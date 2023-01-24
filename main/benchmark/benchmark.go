package main

import (
	"flag"

	"github.com/UNH-DistSyS/UNH-CLT/bench/bench_runtime"
	"github.com/UNH-DistSyS/UNH-CLT/config"
	"github.com/UNH-DistSyS/UNH-CLT/log"
)

var zone = flag.Int("zone", 1, "Zone for the clients in this benchmark instance")
var configFile = flag.String("config", "config.json", "Configuration file for clients. Defaults to config.json.")
var bconfigFile = flag.String("bconfig", "benchmark.json", "Configuration file for benchmark workload. Defaults to benchmark.json.")

func main() {
	flag.Parse()
	cfg := config.LoadConfigFromFile(*configFile)
	bcfg := config.LoadBConfigFromFile(*bconfigFile)
	bench := bench_runtime.NewBenchmark(cfg, bcfg, uint8(*zone))
	if bench != nil {
		bench.Run()
	} else {
		log.Errorf("Could not create Bench")
	}
}
