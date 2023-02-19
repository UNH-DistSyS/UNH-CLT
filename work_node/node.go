package work_node

import (
	"context"
	"crypto/rand"
	"sync"
	"time"

	"github.com/UNH-DistSyS/UNH-CLT/config"
	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/log"
	"github.com/UNH-DistSyS/UNH-CLT/messages"
	"github.com/UNH-DistSyS/UNH-CLT/netwrk"
	"github.com/UNH-DistSyS/UNH-CLT/operation_dispatcher"
	"github.com/UNH-DistSyS/UNH-CLT/utils"
)

type Node struct {
	netman                   netwrk.Communicator
	cfg                      *config.Config
	id                       *ids.ID
	mu                       sync.Mutex
	muRand                   sync.Mutex
	idx                      uint64
	stopCh                   chan bool
	recorded                 uint64 //for testing purporse, will be removed once mearsument is attached.
	latencyTestingInProgress bool
}

func NewNode(cfg *config.Config, identity ids.ID) *Node {
	incomingMsgOperationDispatcher := operation_dispatcher.NewConcurrentOperationDispatcher(identity, cfg.ChanBufferSize, cfg.OpDispatchConcurrency)
	netman := netwrk.NewCommunicator(cfg, identity, incomingMsgOperationDispatcher)

	n := &Node{
		netman:                   netman,
		id:                       &identity,
		stopCh:                   make(chan bool, cfg.ChanBufferSize), //donot block
		idx:                      0,
		latencyTestingInProgress: false,
		cfg:                      cfg,
	}

	n.netman.Register(messages.ConfigMsg{}, n.HandleConfigMsg)
	n.netman.Register(messages.StartLatencyTest{}, n.HandleStartLatencyTestMsg)
	n.netman.Register(messages.StopLatencyTest{}, n.HandleStopLatencyTest)
	n.netman.Register(messages.Ping{}, n.HandlePing) // this handler is for replying node
	return n
}

func (n *Node) Run() {
	n.netman.Run()
}
func (n *Node) Close() {
	n.netman.Close()
}
func (n *Node) stopTesting() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.stopCh <- true
}

func (n *Node) stopAfter(testDuration int) {
	time.Sleep(time.Second * time.Duration(testDuration))
	n.stopTesting()
}

/***********************************************************************************************************************
 * Message Handlers
 **********************************************************************************************************************/

func (n *Node) HandleConfigMsg(ctx context.Context, msg messages.ConfigMsg) {
	n.mu.Lock()
	log.Infof("Node %v received new config msg", n.id)
	n.cfg.PayLoadSize = msg.PayLoadSize
	n.cfg.TestingRateS = msg.TestingRateS
	n.cfg.SelfLoop = msg.SelfLoop
	n.cfg.ClusterMembership.Addrs = msg.Nodes
	n.cfg.ClusterMembership.RefreshIdsFromAddresses()
	n.mu.Unlock()
	msg.C <- messages.ReplyToMaster{
		ID:   msg.ID,
		Ok:   true,
		From: *n.id,
	}
	log.Debugf("Node %v's new cfg: %v", n.id, n.cfg.ClusterMembership.Addrs)
}

func (n *Node) HandleStartLatencyTestMsg(ctx context.Context, msg messages.StartLatencyTest) {
	log.Infof("Node %v trying to start", n.id)
	n.mu.Lock()
	if n.cfg.TestingRateS == 0 {
		// hasn't received cfg yet, wait instead
		log.Errorf("starting with nil cfg")
		n.mu.Unlock()
		msg.C <- messages.ReplyToMaster{
			ID:   msg.ID,
			Ok:   false,
			From: *n.id,
		}
		return
	}

	if n.latencyTestingInProgress {
		log.Errorf("Repeat Starting!")
		n.mu.Unlock()
		msg.C <- messages.ReplyToMaster{
			ID:   msg.ID,
			Ok:   true,
			From: *n.id,
		}
		return
	}
	n.latencyTestingInProgress = true

	go n.senderTicker()
	if msg.TestingDurationSecond > 0 {
		go n.stopAfter(msg.TestingDurationSecond)
	}
	n.mu.Unlock()
	msg.C <- messages.ReplyToMaster{
		ID: msg.ID,
		Ok: true,
	}
}

func (n *Node) HandleStopLatencyTest(ctx context.Context, msg messages.StopLatencyTest) {
	log.Infof("Node %v trying to stop", n.id)
	n.stopTesting()
	msg.C <- messages.ReplyToMaster{
		ID: msg.ID,
		Ok: true,
	}
}

func (n *Node) HandlePing(ctx context.Context, pingMsg messages.Ping) {
	pongMsg := messages.Pong{Payload: pingMsg.Payload, RoundNumber: pingMsg.RoundNumber, ReplyingNodeId: *n.id}
	n.netman.Reply(ctx, pongMsg)
}

/***********************************************************************************************************************
 * Sender (Ping) logic
 **********************************************************************************************************************/

func (n *Node) senderTicker() {
	rate := 1000000 / n.cfg.TestingRateS
	ticker := time.NewTicker(time.Duration(rate * uint64(time.Microsecond)))
	for {
		select {
		case <-n.stopCh:
			//stop testing
			n.mu.Lock()
			n.latencyTestingInProgress = false
			// refreshing channel
			n.stopCh = make(chan bool)
			n.mu.Unlock()

			return
		default:
			<-ticker.C
			timestamp := time.Now().UnixMicro()
			go n.broadcastPing(timestamp, n.idx)
			n.idx++

		}
	}
}

func (n *Node) broadcastPing(startTimeMicroseconds int64, roundnumber uint64) bool {
	log.Debugf("Node %s is sending ping %v", n.id, roundnumber)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(n.cfg.CommunicationTimeoutMs)*time.Millisecond)
	defer cancel()
	payload := make([]byte, n.cfg.PayLoadSize)
	n.muRand.Lock()
	rand.Read(payload)
	n.muRand.Unlock()
	pingMsg := messages.Ping{
		Payload:     payload,
		SenderId:    *n.id,
		RoundNumber: roundnumber,
	}

	respChan, err := n.netman.BroadcastAndAwaitReplies(ctx, n.cfg.SelfLoop, pingMsg) // broadcast ping
	if err != nil {
		log.Errorf("Node %v error starting ping for round %d: %v", n.id, 0, err)
		// give up
		return false
	}

	expectedResponses := len(n.cfg.ClusterMembership.GetIds())
	if !n.cfg.SelfLoop {
		expectedResponses -= 1
	}
	receivedResponses := 0

	for {
		select {
		case pongRaw := <-respChan:
			pong := pongRaw.Body.(messages.Pong)
			n.handlePong(startTimeMicroseconds, pong)
			receivedResponses += 1
			if expectedResponses == receivedResponses {
				return true
			}
		case <-ctx.Done():
			// we hit timeout waiting for replies.
			log.Errorf("Node %v timeout receiving pong responses for round=%d: %v", n.id, 0, ctx.Err())
			return false
		}
	}
}

func (n *Node) handlePong(startTimeMicroseconds int64, pongMsg messages.Pong) {
	// TODO: here we record the measurement

	// The three lines of code are for testing, remove this once measurement is attached.
	n.mu.Lock()
	defer n.mu.Unlock()
	n.recorded = utils.Uint64Max(n.recorded, pongMsg.RoundNumber)
}

func (n *Node) ReturnRecorded() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.recorded
}
