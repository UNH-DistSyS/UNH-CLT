package general_testing

import (
	"github.com/UNH-DistSyS/UNH-CLT/config"
	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/log"
	"github.com/UNH-DistSyS/UNH-CLT/master_node"
	"github.com/UNH-DistSyS/UNH-CLT/work_node"
)

var nodes map[ids.ID]*work_node.Node
var cfg *config.Config
var master master_node.Master
var mid ids.ID
var nids []ids.ID

func SetupMaster() {
	mid = *ids.NewClientID(1, 1)
	log.Infoln("MasterID ", mid)
	master = *master_node.NewMaster(cfg, &mid)
	master.Run()
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
	nodes = make(map[ids.ID]*work_node.Node, len(cfg.ClusterMembership.IDs))
	for _, identity := range nids {
		log.Infof("starting Node %v", identity)
		//conf := config.MakeDefaultConfig()
		workNode := work_node.NewNode(cfg, identity)
		go workNode.Run()
		nodes[identity] = workNode
	}

	log.Infof("Nodes: %v", nodes)
}
