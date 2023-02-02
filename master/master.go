package master

import (
	"github.com/UNH-DistSyS/UNH-CLT/config"
	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/netwrk"
	"github.com/UNH-DistSyS/UNH-CLT/operation_dispatcher"
)

type Master struct {
	netman netwrk.Communicator
	cfg    *config.Config
}

func NewMaster(cfg *config.Config, identity *ids.ID) *Master {
	opDispatcher := operation_dispatcher.NewConcurrentOperationDispatcher(*identity, cfg.ChanBufferSize, cfg.OpDispatchConcurrency)
	netman := netwrk.NewCommunicator(cfg, *identity, opDispatcher)
	return &Master{
		netman: netman,
		cfg:    cfg,
	}
}

func (m *Master) Run() {
	m.netman.Run()
}
