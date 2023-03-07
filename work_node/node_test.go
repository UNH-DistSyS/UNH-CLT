package work_node

// Only run tests one by one, don't run the package. Since there's no method for Nodes to stop.

import (
	"encoding/csv"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/UNH-DistSyS/UNH-CLT/config"
	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/messages"
	"github.com/UNH-DistSyS/UNH-CLT/netwrk"
	"github.com/UNH-DistSyS/UNH-CLT/operation_dispatcher"
	"github.com/stretchr/testify/assert"
)

var nodes []*Node
var cfg *config.Config
var master netwrk.Communicator

func createNode(zone uint8, node uint8) *Node {
	cfg := config.MakeDefaultConfig()
	cfg.TestingRateS = 0
	id := ids.NewID(zone, node)
	return NewNode(cfg, *id)
}

func setupNodeTests(t *testing.T, nodeIds []ids.ID) {
	cfg = config.MakeDefaultConfig()
	cfg.TestingRateS = 1000
	cfg.CommunicationTimeoutMs = 200
	nodes = make([]*Node, 0)
	for i := range nodeIds {
		//cfg.ClusterMembership.Addrs[nodeIds[i]] = "tcp://127.0.0.1:" + strconv.Itoa(config.PORT+i)
		cfg.ClusterMembership.AddNodeAddress(nodeIds[i], "tcp://127.0.0.1:"+strconv.Itoa(config.PORT+i), "tcp://127.0.0.1:"+strconv.Itoa(config.PORT+i))

		dummyNodeCfg := config.MakeDefaultConfig()
		dummyNodeCfg.ClusterMembership.AddNodeAddress(nodeIds[i], "tcp://127.0.0.1:"+strconv.Itoa(config.PORT+i), "tcp://127.0.0.1:"+strconv.Itoa(config.PORT+i))
		node := NewNode(dummyNodeCfg, nodeIds[i])
		go node.Run()
		nodes = append(nodes, node)
	}
	//setting up master
	cfg.ClusterMembership.RefreshIdsFromAddresses()
	cid := ids.NewClientID(1, 1)
	cdispatcher := operation_dispatcher.NewConcurrentOperationDispatcher(*cid, cfg.ChanBufferSize, cfg.OpDispatchConcurrency)
	master = netwrk.NewMasterCommunicator(cfg, *cid, cdispatcher)
	go master.Run()
	cfgMsg := messages.ConfigMsg{}
	cfgMsg.MakeConfigMsg(cfg)
	master.Broadcast(cfgMsg, false)
	time.Sleep(time.Second)
	for i := 0; i < 3; i++ {
		assert.NotEqual(t, uint64(0), nodes[i].cfg.TestingRateS, "Node %v still has old config", nodes[i].id)
	}
}

func TestCreateNode(t *testing.T) {
	node := createNode(1, 1)
	assert.NotEqual(t, nil, node, "Failed to creating Node")
	assert.NotEqual(t, nil, node.cfg, "Failed to creating Node, cfg nil")
	assert.NotEqual(t, nil, node.id, "Failed to creating Node, nil id")
	for i := range nodes {
		nodes[i].Close()
	}
	time.Sleep(time.Second)
}

func TestHandleCfg(t *testing.T) {
	id1 := *ids.GetIDFromString("1.1")
	id2 := *ids.GetIDFromString("1.2")
	id3 := *ids.GetIDFromString("1.3")
	testids := []ids.ID{id1, id2, id3}
	setupNodeTests(t, testids)
	for i := range nodes {
		nodes[i].Close()
	}
}

func TestStartStop(t *testing.T) {
	id1 := *ids.GetIDFromString("1.1")
	id2 := *ids.GetIDFromString("1.2")
	id3 := *ids.GetIDFromString("1.3")
	testids := []ids.ID{id1, id2, id3}
	setupNodeTests(t, testids)

	master.Broadcast(messages.StartLatencyTest{
		ID:                    0,
		TestingDurationSecond: -1}, false)
	time.Sleep(time.Second)
	master.Broadcast(messages.StopLatencyTest{}, false)
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
		idxs3[i] = nodes[i].ReturnRecorded()
	}
	for i := 0; i < 3; i++ {
		assert.NotEqual(t, true, idxs[i] < 100 || idxs[i] != idxs2[i] || idxs2[i] != idxs3[i]+1, "Node %d failed start stop test with %v, %v, %v, %v", i, idxs[i], idxs2[i], idxs3[i], nodes[i].cfg)
	}
	for i := range nodes {
		nodes[i].Close()
	}
}

func TestBroadcastPing(t *testing.T) {
	id1 := *ids.GetIDFromString("1.1")
	id2 := *ids.GetIDFromString("1.2")
	id3 := *ids.GetIDFromString("1.3")
	testids := []ids.ID{id1, id2, id3}
	setupNodeTests(t, testids)
	for i := 0; i < 10; i++ {
		for idx := range nodes {
			ok := nodes[idx].broadcastPing(time.Now().UnixMicro(), uint64(i))
			assert.Equal(t, true, ok, "Node %d failed at %v", nodes[idx].id, i)
		}
	}
	for i := range nodes {
		nodes[i].Close()
	}
}

/***************************************************************************
*					Measurement Integration Tests						   *
****************************************************************************/

var directory string = "raw_data/"

func TestNodeRecordsMeasurement(t *testing.T) {
	//initial setup
	id1 := *ids.GetIDFromString("1.1")
	id2 := *ids.GetIDFromString("1.2")
	id3 := *ids.GetIDFromString("1.3")
	testids := []ids.ID{id1, id2, id3}
	setupNodeTests(t, testids)

	//do some work
	master.Broadcast(messages.StartLatencyTest{
		ID:                    0,
		TestingDurationSecond: -1}, false)
	time.Sleep(time.Second)

	//close nodes and assert csv files created appropriately and not empty
	for i := range nodes {
		nodes[i].Close()

		filePath := directory + nodes[i].cfg.CsvPrefix + "_" + nodes[i].id.String() + "_" + "0.csv"
		f, err := os.Open(filePath)
		assert.Equal(t, nil, err, "Failed to open csv file with path %v", filePath)

		r := csv.NewReader(f)
		_, err = r.Read()
		assert.Equal(t, nil, err, "error: empty file\n")
	}
}
