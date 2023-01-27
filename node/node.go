package node

import (
	"time"

	"github.com/UNH-DistSyS/UNH-CLT/config"
	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/netwrk"
	"github.com/UNH-DistSyS/UNH-CLT/operation_dispatcher"
)

type Node struct {
	netman netwrk.Communicator
}
type Message struct {
	timestamp time.Time
	weight    []string
}

func NewNode(identity *ids.ID, cfg *config.Config) *Node {
	incommingMsgOperationDispatcher := operation_dispatcher.NewConcurrentOperationDispatcher(*identity, cfg.ChanBufferSize, cfg.OpDispatchConcurrency)
	netman := netwrk.NewCommunicator(cfg, *identity, incommingMsgOperationDispatcher)
	return &Node{
		netman: netman,
	}
}

func (n *Node) getConfigFromMaster() {

}

func (n *Node) sendMessage(to ids.ID, m Message) {

}

func (n *Node) sender() {

}

func (n *Node) receiver() {

}

func (n *Node) Run() {
	n.getConfigFromMaster()

	go n.receiver()
	go n.sender()
}
