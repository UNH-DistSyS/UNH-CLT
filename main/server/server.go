package main

import (
	"flag"
	"os"

	"github.com/UNH-DistSyS/UNH-CLT/config"
	"github.com/UNH-DistSyS/UNH-CLT/database"
	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/log"
	"github.com/UNH-DistSyS/UNH-CLT/node_assembler"
)

var memprof = flag.String("memprof", "", "write memory profile to this file")
var configFile = flag.String("config", "bin/config.json", "Configuration file for locusdb replica. Defaults to config.json.")

func main() {
	flag.Parse()
	log.Infof("Loading config %s", *configFile)
	cfg := config.LoadConfigFromFile(*configFile)
	identity := *ids.GetIDFromFlag()
	// As #Algs are growing, makes it eaiser to switch between different algorithms from config, instead of rebuild each time.
	var dbInstance *database.DBNode
	switch cfg.ReplicationProtocol {
	case "mpaxos":
		dbInstance = node_assembler.AssembleDBNodeWithMPaxosReplicator(cfg, identity)
	case "wpaxos":
		dbInstance = node_assembler.AssembleDBNodeWithWPaxosReplicator(cfg, identity)
	default:
		log.Errorf("replication protocol {%v} is not configed \n", cfg.ReplicationProtocol)
		os.Exit(-1)
	}
	dbInstance.Run() // run the DB node
	log.Infof("DB Node %v started\n", dbInstance.NodeId)
	if memprof != nil {
		dbInstance.SetMemProfile(*memprof)
	}
	dbInstance.WaitForTermination() // this will wait for shutdown signal to end this process and all its go-routines
}
