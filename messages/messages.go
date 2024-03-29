package messages

import (
	"encoding/gob"
	"fmt"

	"github.com/UNH-DistSyS/UNH-CLT/config"
	"github.com/UNH-DistSyS/UNH-CLT/ids"
)

func init() {
	// messages
	gob.Register(ConfigMsg{})
	gob.Register(StartLatencyTest{})
	gob.Register(StopLatencyTest{})
	gob.Register(Ping{})
	gob.Register(Pong{})
	gob.Register(ReplyToMaster{})
}

// Node reply message to master_node
type ReplyToMaster struct {
	ID   int
	Ok   bool
	From ids.ID
}

func (r ReplyToMaster) String() string {
	return fmt.Sprintf("ReplyToMaster {MsgID=%d, Ok=%t; Node=%v}", r.ID, r.Ok, r.From)
}

type ConfigMsg struct {
	ID int
	// Cfg config.Config // used to overwrite the default config of the node with a config from master_node
	PayLoadSize           int
	TestingRateS          uint64
	SelfLoop              bool
	Nodes                 map[ids.ID]config.NodeInfo
	TestingDurationMinute int
	C                     chan ReplyToMaster

	CsvPrefix         string
	CSVRowOutputLimit int
	MemRowOutputLimit int
	Compress          bool

	CommunicationTimeoutMs int
}

func NewConfigMsg(cfg *config.Config) ConfigMsg {
	c := new(ConfigMsg)
	c.PayLoadSize = cfg.PayLoadSize
	c.TestingRateS = cfg.TestingRateS
	c.SelfLoop = cfg.SelfLoop
	c.Nodes = cfg.ClusterMembership.Addrs
	c.TestingDurationMinute = cfg.TestingDurationMinute
	c.CSVRowOutputLimit = cfg.CSVRowOutputLimit
	c.MemRowOutputLimit = cfg.MemRowOutputLimit
	c.CsvPrefix = cfg.CsvPrefix
	c.Compress = cfg.Compress
	c.CommunicationTimeoutMs = cfg.CommunicationTimeoutMs
	return *c
}

func (c ConfigMsg) String() string {
	return fmt.Sprintf("ConfigMsg {PayloadSize=%d, TestingRate=%dper second, selfLoop=%t, Nodes=%v, CSVRowOutputLimit=%d, MemRowOutputLimit=%d}", c.PayLoadSize, c.TestingRateS, c.SelfLoop, c.Nodes, c.CSVRowOutputLimit, c.MemRowOutputLimit)
}

type StartLatencyTest struct {
	ID                    int
	TestingDurationSecond int
	C                     chan ReplyToMaster
}

type StopLatencyTest struct {
	ID    int
	Close bool
	C     chan ReplyToMaster
}

type Ping struct {
	Payload     []byte
	SenderId    ids.ID
	RoundNumber uint32
}

func (p Ping) String() string {
	return fmt.Sprintf("Ping {RoundNum=%d, SenderID=%v}", p.RoundNumber, p.SenderId)
}

type Pong struct {
	Payload        []byte
	ReplyingNodeId ids.ID
	RoundNumber    uint32
}

func (p Pong) String() string {
	return fmt.Sprintf("Pong {RoundNum=%d, SenderID=%v}", p.RoundNumber, p.ReplyingNodeId)
}
