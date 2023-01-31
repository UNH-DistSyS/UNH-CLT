package basic_node

import (
	"context"
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

func (n *Node) sendAfter(to ids.ID, m interface{}, thinktime_us int) {
	time.Sleep(time.Duration(thinktime_us) * time.Microsecond)
	n.netman.Send(to, m)

}

func (n *Node) HandlePropose(ctx context.Context, m msg.Propose) {
	go n.doHandlePropose(ctx, m)
}

func (n *Node) doHandlePropose(ctx context.Context, m msg.Propose) {
	// TODO: handle propose message
	msg := msg.Reply{
		ID:        *n.me,
		ProposeID: m.ProposeID,
		Weight:    nil,
		TimeStamp: time.Now(),
	}
	n.netman.Send(m.ID, msg)

}

func (n *Node) HandleReply(ctx context.Context, m msg.Reply) {
	go n.doHandleReply(ctx, m)
}

func (n *Node) doHandleReply(ctx context.Context, m msg.Reply) {
	// TODO: handle reply message, recording two timestamps
}

func (n *Node) HandlerMasterMsg(ctx context.Context, m msg.MasterMsg) {
	switch m.MsgType {
	case msg.CONFIG:
		n.cfg = &m.Cfg
		n.master_id = &m.MasterID
		n.nodes_id = m.NodesID
		n.thinkTime = m.Cfg.ThinkTimeUS
	case msg.RUN:
		n.runtime <- n.cfg.RunTimeS
	case msg.SHUTDOWN:
		n.shutdown <- true
	}

}

func (n *Node) worker(t int, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(time.Second * time.Duration(t))
	for i := uint64(0); ; i++ {
		select {
		case <-ticker.C:
			return
		default:
			m := msg.Propose{
				ID:        *n.me,
				ProposeID: i,
				TimeStamp: time.Now(),
				Weight:    nil,
			}
			n.sendAfter(*n.me, m, n.thinkTime)
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
			for i := 0; i < n.cfg.NumWriter; i++ {
				wg.Add(1)
				go n.worker(t, &wg)
			}
			wg.Wait()
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
