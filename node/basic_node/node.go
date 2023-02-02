package basic_node

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/UNH-DistSyS/UNH-CLT/config"
	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/msg"
	"github.com/UNH-DistSyS/UNH-CLT/netwrk"
	"github.com/UNH-DistSyS/UNH-CLT/operation_dispatcher"
)

type Node struct {
	netman    netwrk.Communicator
	cfg       *config.Config
	me        *ids.ID
	master_id *ids.ID
	nodes_id  []*ids.ID
	thinkTime int
	runtime   chan int
	shutdown  chan bool
	resendLog chan bool
	info      [][]msg.INFO
	mu        sync.Mutex
}

func NewNode(cfg *config.Config, identity *ids.ID) *Node {
	incommingMsgOperationDispatcher := operation_dispatcher.NewConcurrentOperationDispatcher(*identity, cfg.ChanBufferSize, cfg.OpDispatchConcurrency)
	netman := netwrk.NewCommunicator(cfg, *identity, incommingMsgOperationDispatcher)
	node := &Node{
		netman:    netman,
		me:        identity,
		nodes_id:  make([]*ids.ID, 0),
		runtime:   make(chan int),
		shutdown:  make(chan bool),
		resendLog: make(chan bool),
	}
	return node
}

func (n *Node) sendLog(to ids.ID) {
	//TODO: big-file transfering
}

func (n *Node) dealLog() {
	// TODO: sorting and calculating

	// for wid, items := range n.info {
	// 	for _, item := range items {

	// 	}
	// }
}

func (n *Node) HandlePropose(ctx context.Context, m msg.Propose) {
	go n.doHandlePropose(ctx, m)
}

func (n *Node) doHandlePropose(ctx context.Context, m msg.Propose) {
	// TODO: handle propose message
	msg := msg.Reply{
		ID:               *n.me,
		WorkerID:         m.WorkerID,
		ProposeID:        m.ProposeID,
		Weight:           nil,
		TimeStampPropose: m.TimeStampPropose,
		TimeStampReply:   time.Now(),
	}
	n.netman.Send(m.ID, msg)

}

func (n *Node) HandleReply(ctx context.Context, m msg.Reply) {
	go n.doHandleReply(ctx, m)
}

func (n *Node) doHandleReply(ctx context.Context, m msg.Reply) {
	// TODO: handle reply message, recording two timestamps
	to_append := msg.INFO{
		To:               m.ID,
		Weight:           m.Weight,
		ProposeID:        m.ProposeID,
		TimeStampPropose: m.TimeStampPropose,
		TimeStampReply:   m.TimeStampReply,
		TimeStampFinish:  time.Now(),
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	n.info[m.WorkerID] = append(n.info[m.WorkerID], to_append)
}

func (n *Node) HandlerMasterMsg(ctx context.Context, m msg.MasterMsg) {
	switch m.MsgType {
	case msg.CONFIG:
		n.cfg.ClusterMembership = m.Cfg
		n.master_id = &m.MasterID
		n.nodes_id = m.NodesID
		n.thinkTime = m.Cfg.ThinkTimeUS
		n.info = make([][]msg.INFO, n.cfg.ClusterMembership.NumWriter)
		for i := 0; i < n.cfg.ClusterMembership.NumWriter; i++ {
			n.info[i] = make([]msg.INFO, 0)
		}
	case msg.RUN:
		n.runtime <- n.cfg.ClusterMembership.RunTimeS
	case msg.SHUTDOWN:
		n.shutdown <- true
	}
}

func (n *Node) getRandomExcept(notme bool) *ids.ID {
	for {
		rand.Seed(time.Now().UnixNano())
		to_return := n.nodes_id[rand.Intn(len(n.nodes_id))]
		if notme && n.me == to_return {
			continue
		}
		return to_return
	}

}

func (n *Node) worker(id int, t int, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(time.Second * time.Duration(t))
	thinkTicker := time.NewTicker(time.Microsecond * time.Duration(n.cfg.ClusterMembership.ThinkTimeUS))
	for i := uint64(0); ; i++ {
		select {
		case <-ticker.C:
			return
		case <-thinkTicker.C:
			to := n.getRandomExcept(true)
			m := msg.Propose{
				ID:               *n.me,
				WorkerID:         id,
				ProposeID:        i,
				TimeStampPropose: time.Now(),
				Weight:           nil,
			}
			go n.netman.Send(*to, m)
		}
	}

}

func (n *Node) Run() {
	n.netman.Register(msg.MasterMsg{}, n.HandlerMasterMsg)
	n.netman.Register(msg.Propose{}, n.HandlePropose)
	n.netman.Register(msg.Reply{}, n.HandleReply)

	for {
		select {
		case t := <-n.runtime:
			var wg sync.WaitGroup
			for i := 0; i < n.cfg.ClusterMembership.NumWriter; i++ {
				wg.Add(1)
				go n.worker(i, t, &wg)
			}
			wg.Wait()
			n.dealLog()
			n.sendLog(*n.master_id)
		case <-n.resendLog:
			n.sendLog(*n.master_id)
		case <-n.shutdown:
			return
		}
	}
}

//helper functions

// func (n *Node) send(to ids.ID, m interface{}) {
// 	n.netman.Send(to, m)
// }
