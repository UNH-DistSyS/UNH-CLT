package master_node

import (
	"context"
	"sync"
	"time"

	"github.com/UNH-DistSyS/UNH-CLT/config"
	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/log"
	"github.com/UNH-DistSyS/UNH-CLT/messages"
	"github.com/UNH-DistSyS/UNH-CLT/netwrk"
	"github.com/UNH-DistSyS/UNH-CLT/operation_dispatcher"
)

type Master struct {
	netman     netwrk.Communicator
	cfg        *config.Config
	id         ids.ID
	msgID      int
	replyChans map[int]chan bool
	sync.Mutex
}

func NewMaster(cfg *config.Config, identity *ids.ID) *Master {
	opDispatcher := operation_dispatcher.NewConcurrentOperationDispatcher(*identity, cfg.ChanBufferSize, cfg.OpDispatchConcurrency)
	netman := netwrk.NewMasterCommunicator(cfg, *identity, opDispatcher)
	m := Master{
		netman:     netman,
		cfg:        cfg,
		id:         *identity,
		replyChans: make(map[int]chan bool),
	}
	log.Infof("Master is %v", m)
	m.netman.Register(messages.ReplyToMaster{}, m.HandleReply)
	return &m
}

func (m *Master) HandleReply(ctx context.Context, msg messages.ReplyToMaster) {
	m.Lock()
	replyChan := m.replyChans[m.msgID]
	if msg.ID == 0 {
		log.Debugf("HI! %v", m.replyChans)
		replyChan2 := m.replyChans[0]
		if replyChan2 != replyChan {
			log.Errorf("WTF")
		}
	}
	m.Unlock()
	log.Infof("Master %v received reply %v", m.id, msg)
	if replyChan == nil {
		log.Errorf("Reply chan is nil")
	}
	replyChan <- msg.Ok
}

func (m *Master) broadcastMsg(id int, msg interface{}) bool {
	log.Debugf("Master %s is sending msg %v", m.id, msg)
	m.Lock()
	replyCh := make(chan bool, m.cfg.ChanBufferSize)
	log.Debugf("Master making reply chan for msgId %d", id)
	m.replyChans[id] = replyCh
	m.Unlock()
	m.netman.Broadcast(msg, false) // broadcast msg
	expected := len(m.cfg.ClusterMembership.Addrs)
	received := 0
	for {
		select {
		case ok := <-replyCh:
			received++
			log.Debugf("Master received %d OKs", received)
			if !ok {
				m.Lock()
				m.replyChans[id] = nil
				m.Unlock()
				return ok
			}
			if expected <= received {
				m.Lock()
				m.replyChans[id] = nil
				m.Unlock()
				return true
			}
		case <-time.After(time.Millisecond * time.Duration(m.cfg.CommunicationTimeoutMs)):
			log.Debugf("message %v timeout waiting for reply", id)
			m.Lock()
			m.replyChans[id] = nil
			m.Unlock()
			return false
		}
	}
}

func (m *Master) Run() {
	m.netman.Run()
}

func (m *Master) Close() {
	m.netman.Close()
}

func (m *Master) BroadcastConfig() bool {
	m.Mutex.Lock()
	msg := messages.ConfigMsg{
		ID:                    m.msgID,
		PayLoadSize:           m.cfg.PayLoadSize,
		TestingRateS:          m.cfg.TestingRateS,
		SelfLoop:              m.cfg.SelfLoop,
		Nodes:                 m.cfg.ClusterMembership.Addrs,
		TestingDurationMinute: m.cfg.TestingDurationMinute,
	}
	m.msgID++
	m.Mutex.Unlock()
	return m.broadcastMsg(msg.ID, msg)

}
func (m *Master) Start(testDuration int) bool {
	m.Mutex.Lock()
	msg := messages.StartLatencyTest{
		ID:                    m.msgID,
		TestingDurationSecond: testDuration,
	}
	m.msgID++
	m.Mutex.Unlock()
	return m.broadcastMsg(msg.ID, msg)

}

func (m *Master) Stop() bool {
	m.Mutex.Lock()
	msg := messages.StopLatencyTest{
		ID: m.msgID,
	}
	m.msgID++
	m.Mutex.Unlock()
	return m.broadcastMsg(msg.ID, msg)

}
