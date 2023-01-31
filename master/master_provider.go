package master_provider

import (
	"github.com/UNH-DistSyS/UNH-CLT/config"
	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/node/basic_node"
)

type Master interface {
	Run()
}

func NewMasterProvider(cfg *config.Config, identity *ids.ID) Master {
	var master Master
	switch cfg.NodeMode {
	default:
		master = basic_node.NewNode(cfg, identity)
	}
	return master
}
