package general_testing

import (
	"testing"
	"time"

	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/stretchr/testify/assert"
)

func TestConnection(t *testing.T) {
	SetupConfig()
	SetupMaster()
	SetupThreeNodeTest()

	idx1 := make(map[ids.ID]uint64)
	idx2 := make(map[ids.ID]uint64)

	master.Run()
	time.Sleep(time.Second)
	master.Stop()
	time.Sleep(time.Second)

	for id, node := range nodes {
		idx1[id] = node.ReturnRecorded()
	}
	time.Sleep(time.Second)

	for id, node := range nodes {
		idx2[id] = node.ReturnRecorded()
	}
	for id, node := range nodes {
		assert.Greater(t, node.ReturnRecorded(), uint64(100), "Node %v only finished %v commands, which is less than expected", id, node.ReturnRecorded())
		assert.Equal(t, idx1[id], idx2[id], "Node %v failed to stop, %v!=%v", id, idx1[id], idx2[id])
	}
}