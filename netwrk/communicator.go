package netwrk

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/UNH-DistSyS/UNH-CLT/config"
	"github.com/UNH-DistSyS/UNH-CLT/core"
	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/log"
	"github.com/UNH-DistSyS/UNH-CLT/messages"
	"github.com/UNH-DistSyS/UNH-CLT/utils/hlc"
)

const RESPONSE_CHAN_SLACK = 3

// Communicator integrates all networking interface
type Communicator interface {
	core.OperationDispatcher

	// Send put message to outbound queue
	Send(to ids.ID, m interface{})

	// Reply sends a message to the node identified in the context
	Reply(ctx context.Context, m interface{}) error

	// SendAndAwaitReply sends message and awaits a corresponding reply
	SendAndAwaitReply(ctx context.Context, to ids.ID, msg interface{}) (chan Message, error)

	// Broadcast send to all peers
	Broadcast(m interface{}, selfloop bool)

	BroadcastAndAwaitReplies(ctx context.Context, selfloop bool, msg interface{}) (chan Message, error)
}

type TransportLinkManager interface {
	// AddTransportLink adds transportLink to an existing pool of all transports
	AddTransportLink(t TransportLink, to ids.ID)
}

type resendMessage struct {
	originalDestination ids.ID
	message             *Message
}

type basicCommunicator struct {
	sync.RWMutex
	id ids.ID // this node's id

	core.OperationDispatcher      // dispatcher for handling incoming messages
	isMasterSide             bool // whether this communicator is on the master-side of the application
	nodes                    map[ids.ID]TransportLink
	masterListener           TransportLink
	recvChannel              chan *Message       // channel given from communicator to enable zone message relays
	resendChan               chan *resendMessage // channel given from communicator to enable zone message relays
	cfg                      *config.Config      // config object with topology info

	rnd     *rand.Rand
	rndLock sync.RWMutex // Rand is not thread safe, so we have it under lock

	pendingResponses     map[uint64]chan Message // table of pending responses channel. key is cmd id still waiting for
	pendingMutex         sync.RWMutex
	communicationCycleId uint64

	emptyResponseChannels chan chan Message
}

// NewCommunicator returns a Communicator instance given config, self id, and operation dispatcher
func NewCommunicator(cfg *config.Config, nodeId ids.ID, opDispatcher core.OperationDispatcher) Communicator {
	communicator := &basicCommunicator{
		id:                    nodeId,
		isMasterSide:          false,
		cfg:                   cfg,
		nodes:                 make(map[ids.ID]TransportLink),
		recvChannel:           make(chan *Message, cfg.ChanBufferSize),
		resendChan:            make(chan *resendMessage, cfg.ChanBufferSize),
		rnd:                   rand.New(rand.NewSource(time.Now().UnixNano())),
		pendingResponses:      make(map[uint64]chan Message),
		emptyResponseChannels: make(chan chan Message, cfg.ChanBufferSize),
		communicationCycleId:  0,
		OperationDispatcher:   opDispatcher,
	}

	return communicator
}

// NewMasterCommunicator returns a master Communicator instance
// master communicator is used on the master side to send/receive messages to/from nodes
func NewMasterCommunicator(cfg *config.Config, nodeId ids.ID, dispatcher core.OperationDispatcher) Communicator {
	communicator := &basicCommunicator{
		id:                   nodeId,
		isMasterSide:         true,
		nodes:                make(map[ids.ID]TransportLink),
		cfg:                  cfg,
		recvChannel:          make(chan *Message, cfg.ChanBufferSize),
		rnd:                  rand.New(rand.NewSource(time.Now().UnixNano())),
		pendingResponses:     make(map[uint64]chan Message),
		communicationCycleId: 0,
		OperationDispatcher:  dispatcher,
	}

	return communicator
}

/*************************************************************************
 *                TransportLinkManager implementation
 ************************************************************************/

func (c *basicCommunicator) AddTransportLink(t TransportLink, to ids.ID) {
	c.Lock()
	if c.nodes[to] != nil && c.nodes[to].GetLinkState() != StateClosed {
		log.Debugf("Node %v closing existing connection to %v", c.id, to)
		c.nodes[to].Close()
	}
	c.nodes[to] = t

	log.Debugf("Node %v Added %v to list of known nodes", c.id, to)
	c.Unlock()
}

/*************************************************************************
 *                        Communicator Implementation
************************************************************************/

func (c *basicCommunicator) Reply(ctx context.Context, msg interface{}) error {

	meta := ctx.Value(CtxMeta)
	switch ctxMeta := meta.(type) {
	case *core.ContextMeta:
		hlcTime := hlc.HLClock.Now()
		hdr := newMsgHeader(c.id, hlcTime)
		hdr.CycleId = ctxMeta.CurrentMessageCycleId
		// hdr.RequestId = ctxMeta.RequestID
		hdr.Kind = MessageResponse
		c.sendWithHeader(ctxMeta.CurrentMessageSender, msg, hdr)
		return nil
	default:
		return errors.New("bad request header in context")
	}
}

func (c *basicCommunicator) Send(to ids.ID, msg interface{}) {
	hlcTime := hlc.HLClock.Now()
	hdr := newMsgHeader(c.id, hlcTime)
	c.sendWithHeader(to, msg, hdr)
}

func (c *basicCommunicator) SendAndAwaitReply(ctx context.Context, to ids.ID, msg interface{}) (chan Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err() // this should stop sending a message when the context is expired?
	default:
		pendingChan := make(chan Message, 1)
		respChan := make(chan Message, 1)

		cycleId := atomic.AddUint64(&c.communicationCycleId, 1)
		hlcTime := hlc.HLClock.Now()
		hdr := newMsgHeader(c.id, hlcTime).WithCycletId(cycleId)

		c.addPendingChanel(pendingChan, cycleId)
		c.sendWithHeader(to, msg, hdr)

		go func() {
			// TODO: memory leak is possible here if message is never responded to, leading the caller to timeout without cleanup of pending list
			m := <-pendingChan
			respChan <- m
			c.removePendingChanel(cycleId)
		}()

		return respChan, nil
	}
}

func (c *basicCommunicator) Broadcast(msg interface{}, selfloop bool) {
	hlcTime := hlc.HLClock.Now()
	hdr := newMsgHeader(c.id, hlcTime)
	for id := range c.cfg.ClusterMembership.Addrs {
		if id == c.id && !selfloop {
			continue
		}
		log.Infof("Node %v sending %v to %v via broadcast", c.id, id, msg)
		c.sendWithHeader(id, msg, hdr)
	}
}

// BroadcastAndAwaitReplies sends a message msg to all nodes in all zones in the replication region
// and awaits corresponding replies, moving the replicas to a callback function
func (c *basicCommunicator) BroadcastAndAwaitReplies(ctx context.Context, selfloop bool, msg interface{}) (chan Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err() // this should stop sending a message when the context is expired?
	default:

		hlcTime := hlc.HLClock.Now()
		cycleId := atomic.AddUint64(&c.communicationCycleId, 1)
		hdr := newMsgHeader(c.id, hlcTime).WithCycletId(cycleId)
		respChan := c.getAvailableResponseChannel()
		c.addPendingChanel(respChan, cycleId)
		for id := range c.cfg.ClusterMembership.Addrs {
			if id == c.id && !selfloop {
				continue
			}
			c.sendWithHeader(id, msg, hdr)
		}

		// cleanup pending map
		// doneChan := make(chan struct{}, 1)
		go func() {
			<-ctx.Done()
			//log.Debugf("~~~~~~~~~~~CLEAN UP BROADCAST~~~~~~~~~~~~~")
			c.removePendingChanel(cycleId)
		}()

		return respChan, nil
	}
}

// Run the communicator
func (c *basicCommunicator) Run() {
	log.Infof("Starting Communicator at node %v\n", c.id)

	if !c.isMasterSide {
		c.nodes[c.id] = NewTransportLink(c.cfg.ClusterMembership.Addrs[c.id].PrivateAddress, c.id, c.id, c.cfg.ChanBufferSize, c.cfg.CommunicationTimeoutMs, c.resendChan)
		//TODO: add another listener on the public-facing address?
		for _, id := range c.cfg.ClusterMembership.IDs {
			if id != c.id {
				addr := c.cfg.ClusterMembership.Addrs[id].PrivateAddress
				if id.ZoneId != c.id.ZoneId && c.cfg.ClusterMembership.Addrs[id].PublicAddress != "" {
					addr = c.cfg.ClusterMembership.Addrs[id].PublicAddress
				}

				t := NewTransportLink(addr, id, c.id, c.cfg.ChanBufferSize, c.cfg.CommunicationTimeoutMs, c.resendChan)
				c.AddTransportLink(t, id)
			}
		}

		masterListenAddr, err := getMasterAddressFromServer(c.cfg.ClusterMembership.Addrs[c.id].PrivateAddress)
		if err == nil {
			c.masterListener = NewBidirectionalTransportLink(masterListenAddr, c.id, c.id, c.cfg.ChanBufferSize, c.cfg.CommunicationTimeoutMs, c.resendChan, c.recvChannel)
		} else {
			log.Errorf("Error getting master address from node address(%s): %v", masterListenAddr, err)
		}

		// register master message catch-all handler that installs the reply channel
		c.Register(MsgReplyWrapper{}, c.handleMsgReplyWrapper)

		if c.nodes[c.id] != nil {
			c.nodes[c.id].Listen(c.recvChannel)
		}
		if c.masterListener != nil {
			c.masterListener.Listen(c.recvChannel)
		}
	} else {
		for id, cfgAddr := range c.cfg.ClusterMembership.Addrs {
			addr := cfgAddr.PrivateAddress
			if id.ZoneId != c.id.ZoneId && cfgAddr.PublicAddress != "" {
				addr = cfgAddr.PublicAddress
			}

			addr, err := getMasterAddressFromServer(addr)
			if err == nil {
				t := NewBidirectionalTransportLink(addr, id, c.id, c.cfg.ChanBufferSize, c.cfg.CommunicationTimeoutMs, c.resendChan, c.recvChannel)
				c.AddTransportLink(t, id)
			} else {
				log.Errorf("Error getting master address from node address: %v", err)
			}
		}
	}

	c.OperationDispatcher.Run()
	go c.receiveMessages()
	go c.handleResends()
}

func (c *basicCommunicator) Close() {
	c.Lock()
	defer c.Unlock()
	for _, t := range c.nodes {
		t.Close()
	}
	if c.masterListener != nil {
		c.masterListener.Close()
	}
	c.OperationDispatcher.Close()
}

/*************************************************************************
 *                                  Helpers
************************************************************************/

func getMasterAddressFromServer(addr string) (string, error) {
	s := strings.Split(addr, ":")
	if len(s) != 3 {
		return addr, fmt.Errorf("incorrect address specification for address %s", addr)
	}
	port, _ := strconv.Atoi(s[2])
	port += 1000
	return s[0] + ":" + s[1] + ":" + strconv.Itoa(port), nil
}

func (c *basicCommunicator) delaySend(delay time.Duration, to ids.ID, m *Message) {
	go func() {
		t := time.NewTimer(delay)
		<-t.C
		log.Debugf("Node %v resending message %v to node %v", c.id, m, to)
		c.send(to, m)
	}()
}

func (c *basicCommunicator) send(to ids.ID, m *Message) {
	c.RLock()
	t, exists := c.nodes[to]
	c.RUnlock()
	if !exists || t.GetLinkState() == StateClosed {
		addr := c.cfg.ClusterMembership.Addrs[to].PrivateAddress
		if to.ZoneId != c.id.ZoneId && c.cfg.ClusterMembership.Addrs[to].PublicAddress != "" {
			addr = c.cfg.ClusterMembership.Addrs[to].PublicAddress
		}

		if c.isMasterSide {
			t = NewBidirectionalTransportLink(addr, to, c.id, c.cfg.ChanBufferSize, c.cfg.CommunicationTimeoutMs, c.resendChan, c.recvChannel)
		} else {
			t = NewTransportLink(addr, to, c.id, c.cfg.ChanBufferSize, c.cfg.CommunicationTimeoutMs, c.resendChan)
		}
		c.AddTransportLink(t, to)
		//log.Errorf("Communicator on node %v does not have transport for node %s", c.id, to)
		//return
	}
	t.Send(m)
	//log.Debugf("Node %v sent %v to %v", c.id, m, to)
}

func (c *basicCommunicator) sendWithHeader(to ids.ID, msg interface{}, header *MsgHeader) {
	m := &Message{Header: header, Body: msg}
	c.send(to, m)
}

func (c *basicCommunicator) randomIdFromList(list []ids.ID) ids.ID {
	c.rndLock.Lock()
	defer c.rndLock.Unlock()

	r := c.rnd.Intn(len(list))
	for list[r] == c.id {
		r = c.rnd.Intn(len(list))
	}
	return list[r]
}

func (c *basicCommunicator) randomIdFromListWithNoSelfExclusions(list []ids.ID) *ids.ID {
	c.rndLock.Lock()
	defer c.rndLock.Unlock()

	if len(list) == 0 {
		// nothing to pick from
		return nil
	}

	r := c.rnd.Intn(len(list))

	return &list[r]
}

func (c *basicCommunicator) getAvailableResponseChannel() chan Message {
	if len(c.emptyResponseChannels) < RESPONSE_CHAN_SLACK {
		respChan := make(chan Message, c.cfg.ChanBufferSize)
		return respChan
	} else {
		return <-c.emptyResponseChannels
	}
}

func (c *basicCommunicator) addPendingChanel(pending chan Message, cycleId uint64) {
	c.pendingMutex.Lock()
	defer c.pendingMutex.Unlock()
	c.pendingResponses[cycleId] = pending
}

func (c *basicCommunicator) removePendingChanel(cycleId uint64) {
	c.pendingMutex.Lock()
	responseChan := c.pendingResponses[cycleId]
	delete(c.pendingResponses, cycleId)
	c.pendingMutex.Unlock()
	// we want to reuse the response channel to avoid unnecessary memory allocations
	if len(responseChan) == 0 && cap(c.emptyResponseChannels)-len(c.emptyResponseChannels) > RESPONSE_CHAN_SLACK {
		c.emptyResponseChannels <- responseChan
	}
}

func (c *basicCommunicator) handleReplies(m *Message) {
	c.pendingMutex.RLock()
	defer c.pendingMutex.RUnlock()
	if respChan, exists := c.pendingResponses[m.Header.CycleId]; exists {
		respChan <- *m
	} else {
		log.Debugf("Node %v: message header indicates this is a reply, however, no associated request was found. dropping message %v", c.id, m.Body)
	}
}

func (c *basicCommunicator) receiveMessages() {
	for m := range c.recvChannel {
		ctx := context.WithValue(context.Background(), CtxMeta, m.Header.ToContextMeta())

		if m.Header.Kind == MessageResponse && m.Header.CycleId > 0 {
			c.handleReplies(m)
			continue
		}

		c.OperationDispatcher.EnqueueOperation(ctx, m.Body)
	}
}

func (c *basicCommunicator) handleResends() {
	for m := range c.resendChan {
		log.Debugf("Node %v possible resend: %v", c.id, m.message)
		// resend message that is not too old -- i.e., a message that is older than maximum round time should be dropped
		// also resend a message with a ZoneRelay flag regardless of its age.
		if m.message.Header.HLCTime.PhysicalTime+int64(c.cfg.RoundTimeoutMs-c.cfg.CommunicationTimeoutMs) > hlc.HLClock.Now().PhysicalTime {
			c.delaySend(time.Duration(c.cfg.CommunicationTimeoutMs)*time.Millisecond, m.originalDestination, m.message) // resend to the same place
			log.Debugf("Node %v has message %v to node %v in resend channel with new destination of %v", c.id, m.message, m.originalDestination, m.originalDestination)
		}
	}
}

// TODO: Make it more general some time.
func (c *basicCommunicator) handleMsgReplyWrapper(ctx context.Context, m MsgReplyWrapper) {
	switch m.Msg.(type) {
	case messages.ConfigMsg:
		go func() {
			cqlReq := m.Msg.(messages.ConfigMsg)
			cqlReq.C = make(chan messages.ReplyToMaster, 1)
			c.EnqueueOperation(ctx, cqlReq) // we re-enqueue this operation
			cqlReply := <-cqlReq.C
			m.Reply(cqlReply)
			log.Debugf("replied to master")
		}()
	case messages.StartLatencyTest:
		go func() {
			cqlReq := m.Msg.(messages.StartLatencyTest)
			cqlReq.C = make(chan messages.ReplyToMaster, 1)
			c.EnqueueOperation(ctx, cqlReq) // we re-enqueue this operation
			cqlReply := <-cqlReq.C
			m.Reply(cqlReply)
			log.Debugf("replied to master")
		}()
	case messages.StopLatencyTest:
		go func() {
			cqlReq := m.Msg.(messages.StopLatencyTest)
			cqlReq.C = make(chan messages.ReplyToMaster, 1)
			c.EnqueueOperation(ctx, cqlReq) // we re-enqueue this operation
			cqlReply := <-cqlReq.C
			m.Reply(cqlReply)
			log.Debugf("replied to master")
		}()
	}
}
