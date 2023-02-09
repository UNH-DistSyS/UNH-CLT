package node_provider

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/UNH-DistSyS/UNH-CLT/config"
	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/msg"
)

var nodes []Node

func createNode(zone uint8, node uint8) *Node {
	cfg := config.MakeDefaultConfig()
	cfg.TestingRate = 0
	id := ids.NewID(zone, node)
	return NewNode(cfg, id)
}

func setupNodeTests(t *testing.T, nodeIds []ids.ID) {
	cfg := config.MakeDefaultConfig()
	cfg.TestingRate = 0
	cfg.CommunicationTimeoutMs = 200
	for i, id := range nodeIds {
		cfg.ClusterMembership.Addrs[id] = "tcp://127.0.0.1:" + strconv.Itoa(config.PORT+i)
	}
	cfg.ClusterMembership.RefreshIdsFromAddresses()
	for _, id := range nodeIds {
		nodes = append(nodes, *NewNode(cfg, &id))
	}

}

func TestCreateNode(t *testing.T) {
	node := createNode(0, 0)
	if node == nil {
		t.Fatalf("Failed to creating Node")
	}
	if node.cfg == nil {
		t.Fatalf("Failed to creating Node, cfg nil")
	}
	if node.id == nil {
		t.Fatalf("Failed to creating Node, nil id")
	}
}

func TestHandleCfg(t *testing.T) {
	for i := 0; i < 3; i++ {
		nodes = append(nodes, *createNode(0, uint8(i)))
	}
	cfg := config.MakeDefaultConfig()
	cfgMsg := msg.ConfigMsg{
		Cfg: *cfg,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.CommunicationTimeoutMs)*time.Millisecond)
	defer cancel()
	for i := 0; i < 3; i++ {
		nodes[i].HandleConfigMsg(ctx, cfgMsg)
	}
	for i := 0; i < 3; i++ {
		if nodes[i].cfg.TestingRate == 0 {
			t.Fatalf("Node %d still has old config", i)
		}
	}
}

func TestStartStop(t *testing.T) {
	id1 := *ids.GetIDFromString("1.1")
	id2 := *ids.GetIDFromString("1.2")
	id3 := *ids.GetIDFromString("1.3")
	testids := []ids.ID{id1, id2, id3}
	setupNodeTests(t, testids)

	for i := 0; i < 3; i++ {
		nodes[i].Run()
	}
	cfg := config.MakeDefaultConfig()
	cfgMsg := msg.ConfigMsg{
		Cfg: *cfg,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.CommunicationTimeoutMs)*time.Millisecond)
	defer cancel()
	for i := 0; i < 3; i++ {
		nodes[i].HandleConfigMsg(ctx, cfgMsg)
	}
	for i := 0; i < 3; i++ {
		if nodes[i].cfg.TestingRate == 0 {
			t.Fatalf("Node %d still has old config", i)
		}
	}
	for i := 0; i < 3; i++ {
		nodes[i].HandleStartLatencyTestMsg(ctx, msg.StartLatencyTest{})
	}
	time.Sleep(time.Second)
	for i := 0; i < 3; i++ {
		nodes[i].HandleStopLatencyTest(ctx, msg.StopLatencyTest{})
	}
	idxs := make([]uint64, 3)
	for i := 0; i < 3; i++ {
		idxs[i] = nodes[i].idx
	}
	time.Sleep(time.Second)
	idxs2 := make([]uint64, 3)
	for i := 0; i < 3; i++ {
		idxs2[i] = nodes[i].idx
	}

	idxs3 := make([]uint64, 3)
	for i := 0; i < 3; i++ {
		idxs3[i] = nodes[i].recorded
	}
	for i := 0; i < 3; i++ {
		if idxs[i] < 100 || idxs[i] != idxs2[i] || idxs2[i] != idxs3[i]+1 {
			t.Fatalf("Node %d failed start stop test with %v, %v, %v, %v", i, idxs[i], idxs2[i], idxs3[i], nodes)
		}
	}
}
