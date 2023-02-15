package msg

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
}

type ConfigMsg struct {
	// Cfg config.Config // used to overwrite the default config of the node with a config from master
	PayLoadSize  int
	TestingRateS uint64
	SelfLoop     bool
	Addrs        map[ids.ID]string
}

func (c *ConfigMsg) MakeConfigMsg(cfg *config.Config) bool {
	c.PayLoadSize = cfg.PayLoadSize
	c.TestingRateS = cfg.TestingRateS
	c.SelfLoop = cfg.SelfLoop
	c.Addrs = cfg.ClusterMembership.Addrs
	return true
}

func (c ConfigMsg) String() string {
	return fmt.Sprintf("ConfigMsg {Cfg=%v}", c.Addrs)
}

type StartLatencyTest struct {
}

type StopLatencyTest struct {
}

type Ping struct {
	Payload     []byte
	SenderId    ids.ID
	RoundNumber uint64
}

func (p Ping) String() string {
	return fmt.Sprintf("Ping {RoundNum=%d, SenderID=%v}", p.RoundNumber, p.SenderId)
}

type Pong struct {
	Payload        []byte
	ReplyingNodeId ids.ID
	RoundNumber    uint64
}

func (p Pong) String() string {
	return fmt.Sprintf("Pong {RoundNum=%d, SenderID=%v}", p.RoundNumber, p.ReplyingNodeId)
}
