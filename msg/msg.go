package msg

import (
	"time"

	"github.com/UNH-DistSyS/UNH-CLT/config"
	"github.com/UNH-DistSyS/UNH-CLT/ids"
)

const (
	RUN int8 = iota
	CONFIG
	SHUTDOWN
)

type MasterMsg struct {
	MsgType   int8
	Cfg       config.ClusterMembershipConfig
	TimeStamp time.Time
	MasterID  ids.ID
	NodesID   []*ids.ID
}

type LogMsg struct {
	ID   ids.ID
	FILE interface{}
}

type Propose struct {
	ID               ids.ID
	WorkerID         int
	ProposeID        uint64
	TimeStampPropose time.Time
	Weight           []byte
}
type Reply struct {
	ID               ids.ID
	WorkerID         int
	ProposeID        uint64
	TimeStampPropose time.Time
	TimeStampReply   time.Time
	Weight           []byte
}

type INFO struct {
	To               ids.ID
	Weight           []byte
	ProposeID        uint64
	TimeStampPropose time.Time
	TimeStampReply   time.Time
	TimeStampFinish  time.Time
}
