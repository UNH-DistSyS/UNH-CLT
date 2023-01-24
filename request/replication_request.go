package request

import (
	"encoding/gob"
	"fmt"

	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/replication"
	"github.com/UNH-DistSyS/UNH-CLT/store"
)

// OperationReplicationRequest is a Request for replication between nodes
// this is an internal representation of processed INSERT/UPDATE/DELETE
type OperationReplicationRequest struct {
	Command       store.Command
	Timestamp     int64
	ClientZone    uint8
	ForwardedFrom ids.ID
	Forwarded     bool

	C chan OperationReplicationReply
}

// This invokes after the replication is done, so that we can move the results up tp the client-handling layers
// that the operation has been done
func (orr *OperationReplicationRequest) Reply(reply OperationReplicationReply) {
	orr.C <- reply
}

func (orr OperationReplicationRequest) String() string {
	return fmt.Sprintf("OperationReplicationRequest {cmd=%v, forwardedFrom=%v}", orr.Command, orr.ForwardedFrom)
}

// metadata for replication completion
type OperationReplicationReply struct {
	CommandID         ids.CommandID
	ConflictDomainKey store.ByteString
	OperationType     store.OperationType
	Ballot            replication.Ballot // optional ballot to let client know who was responsible for replication

	CmdResults []store.CommandResult // output of an operation that reads some state

	Timestamp int64
	Err       error
}

func (orr OperationReplicationReply) String() string {
	return fmt.Sprintf("ReplicationReply {OpType=%v, cmdID=%v, CDKey=%x, ts=%d}", orr.OperationType, orr.CommandID, orr.ConflictDomainKey, orr.Timestamp)
}

//----------------------------------------------------------------
// Leader change stuff
//----------------------------------------------------------------

type LeaderChangeHint struct {
	HintTime          int64
	ConflictDomainKey store.ByteString
	CurrentBallot     replication.Ballot
	To                ids.ID
	From              ids.ID
}

func (lch LeaderChangeHint) String() string {
	return fmt.Sprintf("LeaderChangeHint {CDKey=%x, to=%v, from=%v}", lch.ConflictDomainKey, lch.To, lch.From)
}

func init() {
	gob.Register(OperationReplicationRequest{})
	gob.Register(OperationReplicationReply{})
	gob.Register(LeaderChangeHint{})
}
