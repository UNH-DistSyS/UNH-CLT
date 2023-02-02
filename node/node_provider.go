package node_provider

import (
	"github.com/UNH-DistSyS/UNH-CLT/config"
	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/node/basic_node"
)

type Node interface {
	Run()
}

func NewNodeProvider(cfg *config.Config, identity *ids.ID) Node {
	var node Node
	switch cfg.NodeMode {
	default:
		node = basic_node.NewNode(cfg, identity)
	}
	return node
}
