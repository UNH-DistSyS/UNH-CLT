package request

import (
	"encoding/gob"
	"fmt"

	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/store"
)

const HTTPHeaderAPI = "api"
const HTTPHeaderAPIKV = "kv"

func init() {
	gob.Register(KVRequest{})
	gob.Register(KVReply{})
}

/***********************************************
 * Client-Replica Messages For ConflictDomainKey-Value Mode  *
 **********************************************/

type KVCommand struct {
	Table           string
	ClientCommandID ids.CommandID
	ClientID        ids.ID
	OperationType   store.OperationType
	Key             store.ByteString
	Value           store.ByteString
}

// IsRead returns true if command is read
func (c KVCommand) IsRead() bool {
	return c.OperationType == store.NOOP
}

// KVRequest is bench request with http response channel
type KVRequest struct {
	Command   KVCommand `json:"command"`
	Timestamp int64     `json:"timestamp"`

	C chan KVReply `json:"-"`
}

// KVReply replies to the current request
func (r *KVRequest) Reply(reply KVReply) {
	r.C <- reply
}

func (r KVRequest) String() string {
	return fmt.Sprintf("KVRequest {cmd=%+v}", r.Command)
}

// KVReply includes all info that might replies to back the client_driver for the corresponding request
type KVReply struct {
	Command   KVCommand
	ClientID  ids.ID
	Value     store.ByteString
	Timestamp int64
	Err       string
}

func (r KVReply) String() string {
	return fmt.Sprintf("KVReply {cmd=%v}", r.Command)
}
