package netwrk

import (
	"bytes"
	"encoding/gob"
	"flag"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/utils"
	"github.com/UNH-DistSyS/UNH-CLT/utils/hlc"

	"github.com/UNH-DistSyS/UNH-CLT/log"
)

type TransportLinkState int

const (
	StateNone TransportLinkState = iota
	StateInitListener
	StateListener // for link that listens for connections
	StateDialing  // for link in the process of dialing to other communicators
	StateDialer   // for link that succesfully dialed to other communicator
	StateClosing
	StateClosed
)

func (m TransportLinkState) String() string {
	return [...]string{"None", "InitListener", "Listener", "Dialing", "Dialer", "Closing", "Closed"}[m]
}

type ModeStateMachine struct {
	TransportLinkState
	sync.RWMutex
}

func NewModeStateMachine() *ModeStateMachine {
	return &ModeStateMachine{
		TransportLinkState: StateNone,
	}
}

func (msm *ModeStateMachine) GetLinkState() TransportLinkState {
	msm.RLock()
	defer msm.RUnlock()
	return msm.TransportLinkState
}

func (msm *ModeStateMachine) TransitionToState(newState TransportLinkState) (TransportLinkState, bool) {
	// this hardcodes the state machine for TransportLink Modes
	msm.Lock()
	defer msm.Unlock()
	oldMode := msm.TransportLinkState
	log.Debugf("Transport link attempts to transition from %v to %v", oldMode, newState)
	return oldMode, msm.transitiontoStateUnderLock(newState)
}

func (msm *ModeStateMachine) transitiontoStateUnderLock(newState TransportLinkState) bool {
	//                |----> StateDialing ------->  StateDialer ----|
	//   StateNone -->|   										    |----> StateClosing ---> StateClosed
	//      |         |----> StateInitListen ---->  StateListener --|                          |
	//      |                                                                                  |
	//      |<---------------------------------------------------------------------------------|
	switch msm.TransportLinkState {
	case StateNone:
		switch newState {
		case StateNone:
			fallthrough
		case StateInitListener:
			fallthrough
		case StateDialing:
			msm.TransportLinkState = newState
			return true
		default:
			return false // invlaid transition
		}
	case StateDialing:
		switch newState {
		case StateNone:
			fallthrough // allow to go back to None
		case StateDialer:
			msm.TransportLinkState = newState
			return true
		default:
			return false // invlaid transition
		}
	case StateDialer:
		switch newState {
		case StateClosing:
			msm.TransportLinkState = newState
			return true
		default:
			return false // invlaid transition
		}
	case StateInitListener:
		switch newState {
		case StateNone:
			fallthrough // allow to go back to None
		case StateListener:
			msm.TransportLinkState = newState
			return true
		default:
			return false // invlaid transition
		}
	case StateListener:
		switch newState {
		case StateClosing:
			msm.TransportLinkState = newState
			return true
		default:
			return false // invlaid transition
		}
	case StateClosing:
		switch newState {
		case StateClosed:
			msm.TransportLinkState = newState
			return true
		default:
			return false // invlaid transition
		}
	case StateClosed:
		switch newState {
		case StateNone:
			msm.TransportLinkState = newState
			return true
		default:
			return false // invlaid transition
		}
	default:
		return false
	}
}

const (
	CtxMeta string = "header"
)

var scheme = flag.String("transportLink", "tcp", "transportLink scheme (tcp, udp, chan), default tcp")

// TransportLink for client & node
type TransportLink interface {
	// Scheme returns transportLink scheme
	Scheme() string

	// GetLinkState returns whether this transportLink is a listener of a dialer
	GetLinkState() TransportLinkState

	// Send sends message into t.send chan
	Send(m *Message)

	// Listen waits for connections and starts receiving messages and discharging them
	// into the operation dispatcher
	// non-blocking once listener starts
	Listen(recvChannel chan *Message)

	// Close closes send channel and stops the listener
	Close()
}

type transportLink struct {
	id                  ids.ID
	endpointId          ids.ID
	isBidirectionalLink bool
	uri                 *url.URL
	chanBufferSize      int
	timeout_ms          int
	send                chan *Message
	recv                chan *Message
	resendChan          chan *resendMessage // channel given from communicator to enable zone message relays
	ModeStateMachine
	listener            net.Listener
	closeListener       chan bool
	incomingConnections []net.Conn
}

// NewTransportLink creates new transportLink for communication between servers.
// Each link is a one-directional communication channel.
// a link can be a dial link or a listener link. Dial links talk to remote server, listener links wait for incomming connections from remote.
// one listener link serves all incomming connections
func NewTransportLink(endpointAddr string, endpointId, nodeId ids.ID, chanBufferSize, timeout_ms int, resendChan chan *resendMessage) TransportLink {
	tl := newBaseTransportLink(endpointAddr, endpointId, nodeId, chanBufferSize, timeout_ms, resendChan, false)
	return baseLinkToProtocolLink(tl)
}

// NewBidirectionalTransportLink. Clients use bidirectional links that expect a reply message for every message received at the Listener link
func NewBidirectionalTransportLink(endpointAddr string, endpointId, nodeId ids.ID, chanBufferSize, timeout_ms int, resendChan chan *resendMessage, recvChannel chan *Message) TransportLink {
	tl := newBaseTransportLink(endpointAddr, endpointId, nodeId, chanBufferSize, timeout_ms, resendChan, true)
	tl.recv = recvChannel
	return baseLinkToProtocolLink(tl)
}

func baseLinkToProtocolLink(tl *transportLink) TransportLink {
	switch tl.Scheme() {
	case "tcp":
		t := new(tcp)
		t.transportLink = tl
		return t
	case "udp":
		t := new(udp)
		t.transportLink = tl
		return t
	default:
		log.Fatalf("unknown communication protocol %s", tl.Scheme())
	}
	return nil
}

func newBaseTransportLink(endpointAddr string, endpointId, nodeId ids.ID, chanBufferSize, timeout_ms int, resendChan chan *resendMessage, biDirectional bool) *transportLink {
	if !strings.Contains(endpointAddr, "://") {
		endpointAddr = *scheme + "://" + endpointAddr
	}
	uri, err := url.Parse(endpointAddr)
	if err != nil {
		log.Fatalf("error parsing address %s : %s\n", endpointAddr, err)
	}

	return &transportLink{
		id:                  nodeId,
		endpointId:          endpointId,
		isBidirectionalLink: biDirectional,
		ModeStateMachine:    *NewModeStateMachine(),
		uri:                 uri,
		chanBufferSize:      chanBufferSize,
		timeout_ms:          timeout_ms,
		send:                make(chan *Message, chanBufferSize),
		resendChan:          resendChan,
		closeListener:       make(chan bool, 1),
		incomingConnections: make([]net.Conn, 0),
	}
}

/*******************************************************
* common transport link methods
********************************************************/

func (t *transportLink) Close() {
	log.Debugf("Node %v closing TL to %v", t.id, t.endpointId)
	if currentMode, ok := t.TransitionToState(StateClosing); ok {
		t.closeInternal(currentMode)
	}
}

func (t *transportLink) drainAndClose() {
	log.Debugf("Node %v draining and closing TL to %v", t.id, t.endpointId)
	if currentMode, ok := t.TransitionToState(StateClosing); ok {
		t.drainSendToResend()
		t.closeInternal(currentMode)
	}
}

func (t *transportLink) closeInternal(currentState TransportLinkState) {
	if currentState == StateListener {
		log.Debugf("Closing transportLink listening on %v at node %v", t.uri, t.id)
	} else {
		log.Debugf("Closing transportLink to %v at node %v", t.uri, t.id)
	}
	if t.listener != nil {
		t.closeListener <- true
		t.listener.Close()
	}

	if currentState == StateListener || t.isBidirectionalLink {
		for _, conn := range t.incomingConnections {
			err := conn.Close()
			if err != nil {
				log.Errorf("Error closing connection: %v", err)
			}
		}
	}

	if currentState != StateListener {
		if t.send != nil {
			close(t.send) // this will terminate the loop consuming the channel
			// reopening closed link is a common occurance, so create a new empty channel for that
			t.send = make(chan *Message, t.chanBufferSize)
		}
	}
	_, ok := t.TransitionToState(StateClosed)
	if ok {
		log.Debugf("Done closing transportLink to %v at node %v", t.uri, t.id)
	} else {
		log.Errorf("Unexpected transition violation while closing transport link on %v", t.id)
	}
}

func (t *transportLink) drainSendToResend() {
	if len(t.send) == 0 {
		log.Debugf("Nothing to drain on node %v TL to %v", t.id, t.endpointId)
		return
	} else {
		for m := range t.send {
			// bubble up the message to communicator for resend logic. We do not want to requeue here
			// since some messages may be resent to a different node entirely
			t.resendChan <- &resendMessage{originalDestination: t.endpointId, message: m} // resend the message
			if len(t.send) == 0 {
				log.Debugf("Drain done on node %v TL to %v", t.id, t.endpointId)
				return
			}
		}
	}
}

func (t *transportLink) drainSend() {
	if len(t.send) == 0 {
		return
	} else {
		for range t.send {
			if len(t.send) == 0 {
				log.Debugf("Drain to void done on node %v outgoing transport to %v", t.id, t.endpointId)
				return
			}
		}
	}
}

func (t *transportLink) Scheme() string {
	return t.uri.Scheme
}

func (t *transportLink) StartOutgoing(conn net.Conn) {
	log.Debugf("Starting Outgoing on node %v TransportLink to %v", t.id, t.endpointId)
	encoder := gob.NewEncoder(conn)
	defer func() {
		log.Debugf("Closing outgoing on node %v", t.id)
		conn.Close()
	}()

	if !t.isBidirectionalLink {
		// non-clients expect nothing in the return channel, so we just read one byte and block the read forever
		// until we get an EOF
		go func() {
			var one = make([]byte, 1)
			_, err := conn.Read(one)
			if err != nil && err.Error() == "EOF" {
				log.Debugf("Node %v TransportLink to %v EOF", t.id, t.endpointId)
				t.drainAndClose()              // drain and shut down
				t.TransitionToState(StateNone) // reset the mode to try again
			}
		}()
	}

	for m := range t.send {
		err := encoder.Encode(&m)
		if err != nil {
			log.Errorf("node %v TransportLink to %v err: %v om msg=%v, send queue size = %d", t.id, t.endpointId, err, m, len(t.send))
			// resend msg m
			t.resendChan <- &resendMessage{originalDestination: t.endpointId, message: m}
			t.drainAndClose()              // drain and shut down
			t.TransitionToState(StateNone) // reset the mode to try again
			return
		}
	}
}

func (t *transportLink) StartIncoming(conn net.Conn) {
	log.Debugf("Waiting for msgs from %v on node %v", conn.RemoteAddr(), t.id)
	decoder := gob.NewDecoder(conn)
	var encoder *gob.Encoder
	numFail := 0
	// we start incoming either as listeners or as dialers on the client side
	startIncomingState := t.GetLinkState()
	for {
		var msg Message
		err := decoder.Decode(&msg)
		if err != nil {
			if err.Error() == "EOF" {
				// the connection has reached EOF, so it was likely closed/aborted. No need to keep trying.
				return
			}
			if t.GetLinkState() != StateClosing {
				log.Errorf("Node %v network error on incomming: %v", t.id, err)
			}
			numFail++
			time.Sleep(10 * time.Millisecond)
			if numFail >= 20 {
				log.Errorf("Too many errors in this link. Stopping connection to remote %v on node %v", conn.RemoteAddr(), t.id)
				return
			}
			continue
		}

		if msg.Header != nil {
			hlc.HLClock.Update(msg.Header.HLCTime)
		}

		//ctx := context.WithValue(context.Background(), CtxMeta, *msg.Header)

		if t.isBidirectionalLink && startIncomingState == StateListener {
			if encoder == nil {
				encoder = gob.NewEncoder(conn)
			}
			cmw := &MsgReplyWrapper{
				Msg: msg.Body,
				C:   nil,
			}
			cmw.SetReplier(encoder)
			msg.Body = *cmw
			t.recv <- &msg
		} else {
			//log.Debugf("Transport on %v received %v", t.id, msg)
			t.recv <- &msg
		}
	}
}

/******************************
/*     TCP communication      *
/******************************/
type tcp struct {
	*transportLink
}

func (t *tcp) Send(m *Message) {
	if t.GetLinkState() == StateNone {
		if oldState, ok := t.TransitionToState(StateDialing); ok {
			go func() {
				// transport was not open for sending yet
				log.Debugf("Node %v is dialing %v", t.id, t.uri)
				err := utils.Retry(t.dial, 5, time.Duration(10)*time.Millisecond)
				log.Debugf("Node %v finished dialing %v", t.id, t.uri)
				if err != nil {
					log.Errorf("Node %v error dialing to remote host: %v. Queue size = %d. m=%v", t.id, err, len(t.send), m)
					if oldState, ok = t.TransitionToState(StateNone); ok {
						t.drainSendToResend()
					} else {
						log.Errorf("unexpected invalid transition to StateDialer from state %v", oldState)
					}
				}
			}()
		} else {
			log.Errorf("Cannot transition to StateDialing from state %v", oldState)
		}
	}
	//log.Debugf("Node %v sending %v", t.id, m)
	t.send <- m
}

func (t *tcp) Listen(recvChannel chan *Message) {
	log.Debugf("start listening id: %v, port: %v, mode: %v", t.id, t.uri.Port(), t.GetLinkState())
	if oldState, ok := t.TransitionToState(StateInitListener); ok {
		t.recv = recvChannel

		var err error
		t.listener, err = net.Listen("tcp", ":"+t.uri.Port())
		if err != nil {
			log.Fatal("TCP Listener error: ", err)
		}
		if oldState, ok := t.TransitionToState(StateListener); ok {
			// if we are in a listener mode, then we do not have a send channel
			// however, a send is still valid as a self-loop send

			go func() {
				// drain send channel into the recv channel
				for m := range t.send {
					t.recv <- m
				}
			}()

			go func(listener net.Listener) {
				for {
					select {
					case <-t.closeListener:
						log.Debugf("Closing listener %v on node %v", listener, t.id)
						return
					default:
						conn, err := listener.Accept()
						if err != nil {
							log.Errorf("TCP Accept error on node %v: %v ", t.id, err)
							continue
						}

						log.Debugf("starting working with an incoming connection on node %v", t.id)
						t.incomingConnections = append(t.incomingConnections, conn)
						go t.StartIncoming(conn)
					}
				}
			}(t.listener)
		} else {
			log.Errorf("Trying to listen on transportLink in mode: %v", oldState)
		}
	} else {
		log.Errorf("Trying to listen on transportLink in mode: %v", oldState)
	}
}

func (t *tcp) dial() error {
	log.Debugf("Dialing %v from node %v", t.uri.Host, t.id)
	conn, err := net.DialTimeout(t.Scheme(), t.uri.Host, time.Duration(t.timeout_ms)*time.Millisecond)
	if err != nil {
		return err
	}
	if oldState, ok := t.TransitionToState(StateDialer); ok {
		// with clients we maintain two-way link
		// unlike cross-node communication which uses one directional links,
		// client dialer also listens to replies
		if t.isBidirectionalLink {
			go t.StartIncoming(conn)
		}

		go t.StartOutgoing(conn)
	} else {
		log.Errorf("unexpected transport link state transition violation on node %v. Expected to transition from StateDialing to StateDialer, but unexpctedly was in state %v", t.id, oldState)
	}

	return nil
}

/******************************
/*     UDP communication      *
/******************************/
type udp struct {
	*transportLink
}

func (u *udp) Send(m *Message) {
	if u.GetLinkState() == StateNone {
		// transport was not open for sending yet
		if oldState, ok := u.TransitionToState(StateDialing); ok {
			log.Debugf("Node %v is dialing %v", u.id, u.uri)
			err := utils.Retry(u.dial, 5, time.Duration(50)*time.Millisecond)
			if err != nil {
				panic(err)
			}
			if oldState, ok = u.TransitionToState(StateDialer); !ok {
				log.Errorf("unexpected state transition violation. Unexpected initial state %v", oldState)
			}
		} else {
			log.Errorf("canot dial from state %v", oldState)
		}
	}
	u.send <- m
}

func (u *udp) Listen(recvChannel chan *Message) {
	u.recv = recvChannel
	addr, err := net.ResolveUDPAddr("udp", ":"+u.uri.Port())
	if err != nil {
		log.Fatal("UDP resolve address error: ", err)
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatal("UDP Listener error: ", err)
	}
	go func(conn *net.UDPConn) {
		packet := make([]byte, 1500)
		defer conn.Close()
		for u.GetLinkState() != StateClosed {
			select {
			case <-u.closeListener:
				return
			default:
				_, err := conn.Read(packet)
				if err != nil {
					log.Error(err)
					continue
				}
				r := bytes.NewReader(packet)
				var m Message
				gob.NewDecoder(r).Decode(&m)
				u.recv <- &m
			}
		}
	}(conn)
}

func (u *udp) dial() error {
	addr, err := net.ResolveUDPAddr("udp", u.uri.Host)
	if err != nil {
		log.Fatal("UDP resolve address error: ", err)
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return err
	}

	go func(conn *net.UDPConn) {
		// packet := make([]byte, 1500)
		// w := bytes.NewBuffer(packet)
		w := new(bytes.Buffer)
		for m := range u.send {
			gob.NewEncoder(w).Encode(&m)
			_, err := conn.Write(w.Bytes())
			if err != nil {
				log.Error(err)
			}
			w.Reset()
		}
	}(conn)

	return nil
}
