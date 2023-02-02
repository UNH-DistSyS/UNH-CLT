package node_provider

import (
	"context"
	"github.com/UNH-DistSyS/UNH-CLT/config"
	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/log"
	"github.com/UNH-DistSyS/UNH-CLT/msg"
	"github.com/UNH-DistSyS/UNH-CLT/netwrk"
	"github.com/UNH-DistSyS/UNH-CLT/operation_dispatcher"
	"sync"
	"time"
)

type Node struct {
	netman netwrk.Communicator
	cfg    *config.Config
	id     *ids.ID
	mu     sync.Mutex
}

func NewNode(cfg *config.Config, identity *ids.ID) *Node {
	incommingMsgOperationDispatcher := operation_dispatcher.NewConcurrentOperationDispatcher(*identity, cfg.ChanBufferSize, cfg.OpDispatchConcurrency)
	netman := netwrk.NewCommunicator(cfg, *identity, incommingMsgOperationDispatcher)

	n := &Node{
		netman: netman,
		id:     identity,
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
}

func (n *Node) HandleStartLatencyTestMsg(ctx context.Context, msg msg.StartLatencyTest) {
	// TODO: start the test. This probably will activate the test ticker
}

func (n *Node) HandleStopLatencyTest(ctx context.Context, msg msg.StopLatencyTest) {
	// TODO: Stop the test. This should stop the ticker loop
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
}

func (n *Node) broadcastPong(startTimeMicroseconds uint64) {
	log.Debugf("Node %s is sending Pong", n.id)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(n.cfg.CommunicationTimeoutMs)*time.Millisecond)
	defer cancel()

	// TODO: fill the ping message
	pingMsg := msg.Ping{
		Payload:     nil,
		SenderId:    *n.id,
		RoundNumber: 0,
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

func (n *Node) handlePong(startTimeMicroseconds uint64, pongMsg msg.Pong) {
	// TODO: here we record the measurement
}
