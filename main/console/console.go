package main

import (
	"flag"
	"fmt"

	"github.com/UNH-DistSyS/UNH-CLT/config"
	"github.com/UNH-DistSyS/UNH-CLT/console"
	"github.com/UNH-DistSyS/UNH-CLT/ids"
)

var configFile = flag.String("config", "bin/config.json", "Configuration file for client. Defaults to config.json.")
var preferredId = flag.String("prefid", "1.1", "Preferred node Id")
var api = flag.String("api", "cql", "Console API")

type Console interface {
	StartConsole()
}

func main() {
	flag.Parse()
	id := ids.GetIDFromFlag()
	cfg := config.LoadConfigFromFile(*configFile)
	if !id.IsClient {
		id = ids.NewClientID(id.ZoneId, id.NodeId)
	}
	prefNodeId := ids.GetIDFromString(*preferredId)
	var consoleInstance Console
	switch *api {
	case "kv":
		consoleInstance = console.NewSimpleKVConsole(cfg, *id, *prefNodeId)
	case "cql":
		consoleInstance = console.NewSimpleCqlHttpConsole(cfg, *id, *prefNodeId)
	default:
		fmt.Errorf("specify Console API: 'kv' or 'cql'")
		return
	}
	fmt.Printf("Welcome to LocusDB Console! Preferred Node: %v. This client ID: %v\n", prefNodeId, id)
	consoleInstance.StartConsole()
}
