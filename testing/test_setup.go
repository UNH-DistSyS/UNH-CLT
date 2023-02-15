package general_testing

import (
	"fmt"

	"github.com/UNH-DistSyS/UNH-CLT/config"
	"github.com/UNH-DistSyS/UNH-CLT/ids"
	master_provider "github.com/UNH-DistSyS/UNH-CLT/master"
	node_provider "github.com/UNH-DistSyS/UNH-CLT/node"
)

var nodes map[ids.ID]*node_provider.Node
var cfg *config.Config
var master master_provider.Master
var mid ids.ID
var nids []ids.ID

func SetupMaster() {
	mid = *ids.NewClientID(1, 0)
	fmt.Println("MasterID ", mid)
	master = *master_provider.NewMaster(cfg, &mid)
}

func SetupConfig() {
	cfg = config.LoadConfigFromFile("config.json")
}

func SetupThreeNodeTest() {

	id1 := *ids.GetIDFromString("1.1")
	id2 := *ids.GetIDFromString("1.2")
	id3 := *ids.GetIDFromString("1.3")
	testids := []ids.ID{id1, id2, id3}
	nids = testids
	nodes = make(map[ids.ID]*node_provider.Node, len(cfg.ClusterMembership.IDs))
	for _, identity := range nids {
		// log.Infof("starting Node %v with cfg: %v", identity, cfg)
		node := node_provider.NewNode(cfg, &identity)
		node.Run()
		nodes[identity] = node
	}

}
