package node_provider

import (
	"context"
	"crypto/rand"
	"sync"
	"time"

	"github.com/UNH-DistSyS/UNH-CLT/config"
	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/log"
	"github.com/UNH-DistSyS/UNH-CLT/msg"
	"github.com/UNH-DistSyS/UNH-CLT/netwrk"
	"github.com/UNH-DistSyS/UNH-CLT/operation_dispatcher"
	"github.com/UNH-DistSyS/UNH-CLT/utils"
)

type Node struct {
	netman   netwrk.Communicator
	cfg      *config.Config
	id       *ids.ID
	mu       sync.Mutex
	idx      uint64
	stopCh   chan bool
	recorded uint64 //for testing
	testing  bool
}

func NewNode(cfg *config.Config, identity *ids.ID) *Node {
	incommingMsgOperationDispatcher := operation_dispatcher.NewConcurrentOperationDispatcher(*identity, cfg.ChanBufferSize, cfg.OpDispatchConcurrency)
	netman := netwrk.NewCommunicator(cfg, *identity, incommingMsgOperationDispatcher)

	n := &Node{
		netman:  netman,
		id:      identity,
		stopCh:  make(chan bool),
		idx:     0,
		testing: false,
		cfg:     cfg,
	}

	n.netman.Register(msg.ConfigMsg{}, n.HandleConfigMsg)
	n.netman.Register(msg.StartLatencyTest{}, n.HandleStartLatencyTestMsg)
	n.netman.Register(msg.StopLatencyTest{}, n.HandleStopLatencyTest)
	n.netman.Register(msg.Ping{}, n.HandlePing) // this handler is for replying node

	return n
}

func (n *Node) Run() {
	n.netman.Run()

}

/***********************************************************************************************************************
 * Message Handlers
 **********************************************************************************************************************/

func (n *Node) HandleConfigMsg(ctx context.Context, msg msg.ConfigMsg) {
	n.mu.Lock()
	defer n.mu.Unlock()
	// TODO: manually merge received config into the default one. Do not just assign received config to n.Cfg
	n.cfg.PayLoadSize = msg.Cfg.PayLoadSize
	n.cfg.TestingRate = msg.Cfg.TestingRate
	n.cfg.SelfLoop = msg.Cfg.SelfLoop
	n.netman.Reply(ctx, msg)
}

func (n *Node) HandleStartLatencyTestMsg(ctx context.Context, msg msg.StartLatencyTest) {
	// TODO: start the test. This probably will activate the test ticker
	if n.cfg == nil {
		// hasn't received cfg yet, wait instead
		log.Errorf("starting with nil cfg")
		return
	}
	n.mu.Lock()
	if n.testing {
		n.mu.Unlock()
		log.Errorf("Repeat Starting!")
		return
	}
	n.testing = true
	n.mu.Unlock()
	go n.senderTicker()
}

func (n *Node) HandleStopLatencyTest(ctx context.Context, msg msg.StopLatencyTest) {
	// TODO: Stop the test. This should stop the ticker loop
	n.stopCh <- true
}

func (n *Node) HandlePing(ctx context.Context, pingMsg msg.Ping) {
	pongMsg := msg.Pong{Payload: pingMsg.Payload, RoundNumber: pingMsg.RoundNumber, ReplyingNodeId: *n.id}
	n.netman.Reply(ctx, pongMsg)
}

/***********************************************************************************************************************
 * Sender (Ping) logic
 **********************************************************************************************************************/

func (n *Node) senderTicker() {
	// TODO: ticker loop. The loop will call broadcastPong at constant rate
	rate := 1000 / n.cfg.TestingRate
	ticker := time.NewTicker(time.Duration(rate * uint64(time.Microsecond)))
	for {
		select {
		case <-n.stopCh:
			//stop testing
			n.mu.Lock()
			n.testing = false
			n.mu.Unlock()
			return
		default:
			<-ticker.C
			timestamp := time.Now().UnixMicro()
			go n.broadcastPong(timestamp, n.idx)
			n.idx++

		}
	}
}

func (n *Node) broadcastPong(startTimeMicroseconds int64, roundnumber uint64) {
	log.Debugf("Node %s is sending Pong", n.id)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(n.cfg.CommunicationTimeoutMs)*time.Millisecond)
	defer cancel()
	payload := make([]byte, n.cfg.PayLoadSize)
	rand.Read(payload)
	// TODO: fill the ping message
	pingMsg := msg.Ping{
		Payload:     payload,
		SenderId:    *n.id,
		RoundNumber: roundnumber,
	}

	respChan, err := n.netman.BroadcastAndAwaitReplies(ctx, n.cfg.SelfLoop, pingMsg) // broadcast ping
	if err != nil {
		log.Errorf("Node %v error starting ping for round %d: %v", n.id, 0, err)
		// give up
		return
	}

	expectedResponses := len(n.cfg.ClusterMembership.GetIds())
	if !n.cfg.SelfLoop {
		expectedResponses -= 1
	}
	receivedResponses := 0

	for {
		select {
		case pongRaw := <-respChan:
			pong := pongRaw.Body.(msg.Pong)
			n.handlePong(startTimeMicroseconds, pong)
			receivedResponses += 1
			if expectedResponses == receivedResponses {
				return
			}
		case <-ctx.Done():
			// we hit timeout waiting for replies.
			log.Errorf("Node %v timeout receiving pong responses for round=%d: %v", n.id, 0, ctx.Err())
			return
		}
	}
}

func (n *Node) handlePong(startTimeMicroseconds int64, pongMsg msg.Pong) {
	// TODO: here we record the measurement
	n.mu.Lock()
	defer n.mu.Unlock()
	n.recorded = utils.Uint64Max(n.recorded, pongMsg.RoundNumber)
}
