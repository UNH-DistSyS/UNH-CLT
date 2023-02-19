package netwrk

import (
	"fmt"

	"github.com/UNH-DistSyS/UNH-CLT/core"
	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/utils/hlc"
)

/*------------------------------------------------
 * Message Header contains supporting information about the message,
 * like HLC time of the message sender at the time of sendMsg and
 * whether this message needs a RegionalRelay
 *------------------------------------------------*/

type MessageKind byte

const (
	MessageNormal MessageKind = iota
	MessageResponse
)

type MsgHeader struct {
	Sender  ids.ID
	HLCTime hlc.Timestamp

	Kind    MessageKind
	CycleId uint64 // id of communication cycle if part of fixed response-reply loop
}

func newMsgHeader(senderId ids.ID, hlcTime hlc.Timestamp) *MsgHeader {
	return &MsgHeader{
		Sender:  senderId,
		HLCTime: hlcTime,
	}
}

func (hdr *MsgHeader) SetKind(kind MessageKind) *MsgHeader {
	hdr.Kind = kind
	return hdr
}

// func (hdr *MsgHeader) WithRequestId(reqId ids.CommandID) *MsgHeader {
// 	hdr.RequestId = reqId
// 	return hdr
// }

func (hdr *MsgHeader) WithCycletId(cycleId uint64) *MsgHeader {
	hdr.CycleId = cycleId
	return hdr
}

func (hdr MsgHeader) String() string {
	return fmt.Sprintf("MSG HEADER {hlcint=%d}", hdr.HLCTime)
}

func (hdr *MsgHeader) ToBytes() []byte {
	return hdr.HLCTime.ToBytes()
}

func (hdr *MsgHeader) ToContextMeta() *core.ContextMeta {
	return &core.ContextMeta{
		// RequestID:             hdr.RequestId,
		CurrentMessageSender:  hdr.Sender,
		CurrentMessageCycleId: hdr.CycleId,
	}
}

func NewMsgHeaderFromBytes(bts []byte) *MsgHeader {
	ts := hlc.NewTimestampBytes(bts)
	hdr := MsgHeader{HLCTime: *ts}
	return &hdr
}
