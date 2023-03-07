package general_testing

import (
	"io/ioutil"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/stretchr/testify/assert"
)

func TestStartStop(t *testing.T) {
	SetupConfig()
	SetupMaster()
	SetupThreeNodeTest()

	idx1 := make(map[ids.ID]uint64)
	idx2 := make(map[ids.ID]uint64)

	master.Start(-1)
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
	for _, node := range nodes {
		node.Close()
	}
	master.Close()
	time.Sleep(time.Second)
}

func TestOneSecond(t *testing.T) {
	SetupConfig()
	SetupMaster()
	SetupThreeNodeTest()

	idx1 := make(map[ids.ID]uint64)
	idx2 := make(map[ids.ID]uint64)

	master.Start(1)
	time.Sleep(2 * time.Second)
	for id, node := range nodes {
		idx1[id] = node.ReturnRecorded()
	}
	time.Sleep(time.Second)

	for id, node := range nodes {
		idx2[id] = node.ReturnRecorded()
	}
	for id, node := range nodes {
		assert.Greater(t, node.ReturnRecorded(), uint64(700), "Node %v only finished %v commands, which is less than expected", id, node.ReturnRecorded())
		assert.Equal(t, idx1[id], idx2[id], "Node %v failed to stop, %v!=%v", id, idx1[id], idx2[id])
	}
	for _, node := range nodes {
		node.Close()
	}
	master.Close()
	time.Sleep(time.Second)
}

func Test_GetIdx(t *testing.T) {
	SetupConfig()
	SetupMaster()
	SetupThreeNodeTest()

	file_content := "thisNodeId,roundNumber,remoteNodeId,startTime,endTime\n1.1,0,1.1,4476361270299,4476361270406"
	for i := 0; i < 3; i++ {
		for _, id := range nids {
			file, _ := os.Create("/Users/noahcui/test_files/" + id.String() + "/testing_" + id.String() + "_" + strconv.Itoa(i) + ".csv")
			file.WriteString(file_content)
			file.Close()
		}
	}

	for _, id := range nids {
		start, end := master.GetFileIdxes(id)
		assert.Equal(t, 0, start, "start: %v", start)
		assert.Equal(t, 2, end, "end: %v", end)
	}
	for _, node := range nodes {
		node.Close()
	}
	master.Close()
}
func Test_Download(t *testing.T) {
	SetupConfig()
	SetupMaster()
	SetupThreeNodeTest()
	file_content := "thisNodeId,roundNumber,remoteNodeId,startTime,endTime\n1.1,0,1.1,4476361270299,4476361270406"
	for i := 0; i < 3; i++ {
		for _, id := range nids {
			file, _ := os.Create("/Users/noahcui/test_files/" + id.String() + "/testing_" + id.String() + "_" + strconv.Itoa(i) + ".csv")
			file.WriteString(file_content)
			file.Close()
		}
	}

	var start, end int
	for _, id := range nids {
		start, end = master.GetFileIdxes(id)
		assert.Equal(t, 0, start, "start: %v", start)
		assert.Equal(t, 2, end, "end: %v", end)
	}

	for _, id := range nids {
		master.CopyFileFromRemoteToLocal(id, start, end, id.String()+"/")
	}

	for i := 0; i < 3; i++ {
		for _, id := range nids {
			filename := "/Users/noahcui/test_files/" + id.String() + "/testing_" + id.String() + "_" + strconv.Itoa(i) + ".csv"
			content, _ := ioutil.ReadFile(filename)
			assert.Equal(t, file_content, string(content))
		}
	}

	for _, id := range nids {
		master.DeleteFile(id, start, end)
		s1, e1 := master.GetFileIdxes(id)
		assert.Equal(t, s1, 2)
		assert.Equal(t, e1, 2)
	}

	for _, node := range nodes {
		node.Close()
	}
	master.Close()
}
