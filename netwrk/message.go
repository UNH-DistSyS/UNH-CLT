package netwrk

import (
	"encoding/gob"
	"fmt"

	"github.com/UNH-DistSyS/UNH-CLT/log"
	"github.com/UNH-DistSyS/UNH-CLT/utils/hlc"
)

func init() {
	gob.Register(MsgHeader{})
	gob.Register(Message{})
}

/***************************
 * Transport layer message
 * combines Header and body
 ***************************/

type Message struct {
	Header *MsgHeader
	Body   interface{}

	C chan Message // reply channel to reply to client. This is ignored for inter-node communication
}

/***************************
 * Client-Replica Messages *
 ***************************/

// generic client protocol msg with HLC
type MsgReplyWrapper struct {
	Msg interface{}
	C   chan interface{} // reply channel created by request receiver
}

// Reply replies to current client session
func (c *MsgReplyWrapper) Reply(reply interface{}) {
	c.C <- reply
}

func (c *MsgReplyWrapper) SetReplier(encoder *gob.Encoder) {
	c.C = make(chan interface{}, 1)
	go func(r *MsgReplyWrapper) {
		replyBody := <-c.C
		hlcTime := hlc.HLClock.Now()
		hdr := &MsgHeader{HLCTime: hlcTime}
		pm := Message{Header: hdr, Body: replyBody}
		log.Debugf("sending reply %v", pm)
		err := encoder.Encode(&pm)
		if err != nil {
			log.Errorf("Error replying to client: %v", err)
		}
	}(c)
}

func (c MsgReplyWrapper) String() string {
	return fmt.Sprintf("MsgReplyWrapper {msg=%v}", c.Msg)
}
