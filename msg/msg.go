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
	Cfg       config.Config
	TimeStamp time.Time
	MasterID  ids.ID
	NodesID   []*ids.ID
}

type LogMsg struct {
	ID   ids.ID
	FILE interface{}
}

type Propose struct {
	ID        ids.ID
	ProposeID uint64
	TimeStamp time.Time
	Weight    []string
}
type Reply struct {
	ID        ids.ID
	ProposeID uint64
	TimeStamp time.Time
	Weight    []string
}
