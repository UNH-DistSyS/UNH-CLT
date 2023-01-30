package master

import (
	"github.com/UNH-DistSyS/UNH-CLT/config"
	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/netwrk"
	"github.com/UNH-DistSyS/UNH-CLT/operation_dispatcher"
)

type Master struct {
	netman    netwrk.Communicator
	cfg       *config.Config
	nodecfg   map[ids.ID]*config.ClusterMembershipConfig
	nodeAddrs map[ids.ID]string
	nodePorts map[ids.ID]string
}

func NewMaster(cfg *config.Config, identity *ids.ID) *Master {
	incommingMsgOperationDispatcher := operation_dispatcher.NewConcurrentOperationDispatcher(*identity, cfg.ChanBufferSize, cfg.OpDispatchConcurrency)
	netman := netwrk.NewCommunicator(cfg, *identity, incommingMsgOperationDispatcher)
	return &Master{
		netman:    netman,
		cfg:       cfg,
		nodecfg:   make(map[ids.ID]*config.ClusterMembershipConfig),
		nodeAddrs: make(map[ids.ID]string),
		nodePorts: make(map[ids.ID]string),
	}
}

func (m *Master) ConnectNodes() {
}

func (m *Master) BroadcastConfigs() {

}

func (m *Master) TestForTime() {

}

func (m *Master) Stop() {

}

func (m *Master) WaitForResults() {

}

func (m *Master) RetriveData() {

}

func (m *Master) Run() {
	m.ConnectNodes()
	m.BroadcastConfigs()
	m.TestForTime()
	m.WaitForResults()
}
