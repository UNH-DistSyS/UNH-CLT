package basic_master

import (
	"context"
	"sync"
	"time"

	"github.com/UNH-DistSyS/UNH-CLT/config"
	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/log"
	"github.com/UNH-DistSyS/UNH-CLT/msg"
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

var wg sync.WaitGroup

func (master *Master) HandleLogFile(ctx, m msg.LogMsg) {
	// Handle File

	// If success
	wg.Done()
}
func (master *Master) Run() {
	master.netman.Register(msg.LogMsg{}, master.HandleLogFile)

	m := msg.MasterMsg{
		MsgType: msg.CONFIG,
		Cfg:     *master.cfg,
	}
	var ctx context.Context
	_, err := master.netman.BroadcastAndAwaitReplies(ctx, false, m)
	if err != nil {
		log.Infof("error broadcasting msg")
		return
	}
	time.Sleep(time.Second)
	m.MsgType = msg.RUN

	wg.Add(master.cfg.NumNode)
	wg.Wait()
	m.MsgType = msg.SHUTDOWN
	master.netman.Broadcast(m, false)
}
