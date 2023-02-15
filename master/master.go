package master_provider

import (
	"context"
	"fmt"
	"time"

	"github.com/UNH-DistSyS/UNH-CLT/config"
	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/log"
	"github.com/UNH-DistSyS/UNH-CLT/msg"
	"github.com/UNH-DistSyS/UNH-CLT/netwrk"
	"github.com/UNH-DistSyS/UNH-CLT/operation_dispatcher"
)

type Master struct {
	netman netwrk.Communicator
	cfg    *config.Config
	id     ids.ID
}

func NewMaster(cfg *config.Config, identity *ids.ID) *Master {
	opDispatcher := operation_dispatcher.NewConcurrentOperationDispatcher(*identity, cfg.ChanBufferSize, cfg.OpDispatchConcurrency)
	netman := netwrk.NewCommunicator(cfg, *identity, opDispatcher)
	m := Master{
		netman: netman,
		cfg:    cfg,
		id:     *identity,
	}
	fmt.Println("Master is", m)
	return &m
}

func (m *Master) broadcastMsg(msg interface{}) bool {
	log.Debugf("Master %s is sending msg", m.id)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(m.cfg.CommunicationTimeoutMs)*time.Millisecond)
	defer cancel()

	respChan, err := m.netman.BroadcastAndAwaitReplies(ctx, false, msg) // broadcast msg

	if err != nil {
		log.Errorf("Master %v error sending msg: %v", m.id, err)
		// give up
		return false
	}

	// Master do not self loop
	expectedResponses := len(m.cfg.ClusterMembership.GetIds())
	receivedResponses := 0

	for {
		select {
		case <-respChan:
			receivedResponses += 1
			if expectedResponses == receivedResponses {
				return true
			}
		case <-ctx.Done():
			// always time out since there's no id space for master in clusterMembership.address
			log.Errorf("Master %v timeout receiving responses for msg=%v: %v, received=%v", m.id, msg, ctx.Err(), receivedResponses)
			return false
		}
	}
}

func (m *Master) Run() {
	m.netman.Run()
	// time.Sleep(time.Second)
	cfgMsg := msg.ConfigMsg{
		PayLoadSize:  m.cfg.PayLoadSize,
		TestingRateS: m.cfg.TestingRateS,
		SelfLoop:     m.cfg.SelfLoop,
		Addrs:        m.cfg.ClusterMembership.Addrs,
	}
	// cfgMsg.MakeConfigMsg(m.cfg)

	m.broadcastMsg(cfgMsg)

	m.broadcastMsg(msg.StartLatencyTest{})

	m.netman.Close()
}

func (m *Master) Stop() {
	m.netman.Run()

	m.broadcastMsg(msg.StopLatencyTest{})

}
