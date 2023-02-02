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
	Cfg config.Config // used to overwrite the default config of the node with a config from master
}

func (c ConfigMsg) String() string {
	return fmt.Sprintf("ConfigMsg {Cfg=%v}", c.Cfg)
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
