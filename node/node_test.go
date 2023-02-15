package node_provider

// Only run tests one by one, don't run the package. Since there's no method for Nodes to stop.

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/UNH-DistSyS/UNH-CLT/config"
	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/msg"
	"github.com/stretchr/testify/assert"
)

var nodes []Node

func createNode(zone uint8, node uint8) *Node {
	cfg := config.MakeDefaultConfig()
	cfg.TestingRateS = 0
	id := ids.NewID(zone, node)
	return NewNode(cfg, id)
}

func setupNodeTests(t *testing.T, nodeIds []ids.ID) {
	cfg := config.MakeDefaultConfig()
	cfg.TestingRateS = 0
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
	nodes = make([]Node, 0)
	node := createNode(0, 0)
	assert.NotEqual(t, nil, node, "Failed to creating Node")
	assert.NotEqual(t, nil, node.cfg, "Failed to creating Node, cfg nil")
	assert.NotEqual(t, nil, node.id, "Failed to creating Node, nil id")
}

func TestHandleCfg(t *testing.T) {
	nodes = make([]Node, 0)
	id1 := *ids.GetIDFromString("1.1")
	id2 := *ids.GetIDFromString("1.2")
	id3 := *ids.GetIDFromString("1.3")
	testids := []ids.ID{id1, id2, id3}
	setupNodeTests(t, testids)
	for _, node := range nodes {
		node.Run()
	}
	cfg := config.MakeDefaultConfig()
	cfg.TestingRateS = 1000
	cfgMsg := msg.ConfigMsg{}
	cfgMsg.MakeConfigMsg(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.CommunicationTimeoutMs)*time.Millisecond)
	defer cancel()
	for i := 0; i < 3; i++ {
		nodes[i].HandleConfigMsg(ctx, cfgMsg)
	}
	for i := 0; i < 3; i++ {
		assert.NotEqual(t, 0, nodes[i].cfg.TestingRateS, "Node %d still has old config", i)
	}
}

func TestStartStop(t *testing.T) {
	nodes = make([]Node, 0)
	id1 := *ids.GetIDFromString("1.1")
	id2 := *ids.GetIDFromString("1.2")
	id3 := *ids.GetIDFromString("1.3")
	testids := []ids.ID{id1, id2, id3}
	setupNodeTests(t, testids)

	for i := 0; i < 3; i++ {
		nodes[i].Run()
	}
	cfg := config.MakeDefaultConfig()
	cfg.TestingRateS = 1000
	cfgMsg := msg.ConfigMsg{}
	cfgMsg.MakeConfigMsg(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.CommunicationTimeoutMs)*time.Millisecond)
	defer cancel()
	for i := 0; i < 3; i++ {
		nodes[i].HandleConfigMsg(ctx, cfgMsg)
	}
	for i := 0; i < 3; i++ {
		assert.NotEqual(t, 0, nodes[i].cfg.TestingRateS, "Node %d still has old config", i)
	}
	for i := 0; i < 3; i++ {
		nodes[i].HandleStartLatencyTestMsg(ctx, msg.StartLatencyTest{})
	}
	time.Sleep(time.Second)
	for i := 0; i < 3; i++ {
		nodes[i].HandleStopLatencyTest(ctx, msg.StopLatencyTest{})
	}
	time.Sleep(time.Second)
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
		assert.NotEqual(t, true, idxs[i] < 100 || idxs[i] != idxs2[i] || idxs2[i] != idxs3[i]+1, "Node %d failed start stop test with %v, %v, %v, %v", i, idxs[i], idxs2[i], idxs3[i], nodes)
	}
}

func TestBroadcastPing(t *testing.T) {
	nodes = make([]Node, 0)
	id1 := *ids.GetIDFromString("1.1")
	id2 := *ids.GetIDFromString("1.2")
	id3 := *ids.GetIDFromString("1.3")
	testids := []ids.ID{id1, id2, id3}
	setupNodeTests(t, testids)

	for i := 0; i < 3; i++ {
		nodes[i].Run()
	}
	cfg := config.MakeDefaultConfig()
	cfg.TestingRateS = 1000
	cfgMsg := msg.ConfigMsg{}
	cfgMsg.MakeConfigMsg(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.CommunicationTimeoutMs)*time.Millisecond)
	defer cancel()
	for i := 0; i < 3; i++ {
		nodes[i].HandleConfigMsg(ctx, cfgMsg)
	}
	for i := 0; i < 3; i++ {
		assert.NotEqual(t, 0, nodes[i].cfg.TestingRateS, "Node %d still has old config", i)

	}
	for i := 0; i < 10; i++ {
		for idx, node := range nodes {
			ok := node.broadcastPing(time.Now().UnixMicro(), uint64(i))
			assert.Equal(t, true, ok, "Node %d failed at %v", idx, i)
		}
	}
}
