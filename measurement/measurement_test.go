package measurement

import (
	"encoding/csv"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/log"
	"github.com/stretchr/testify/assert"
)

/***********************************************************************************************************************
 * Helper functions/variables
 **********************************************************************************************************************/

var DEBUG = true
var testPrefix = "test"

func CreateMeasurement(listSizeTestParam int) *Measurement {
	return NewMeasurement(ids.GetIDFromFlag(), testPrefix, listSizeTestParam)
}

// Produces random amount of "time" between
// 10 - 30 mcs to simulate request work
func SimulateWork() int64 {
	rand.Seed(time.Now().UnixNano())
	min := int64(10)
	max := int64(30)

	return rand.Int63n(max-min+1) + min
}

func DoMeasurement(m *Measurement, remoteId *ids.ID) (int64, int64) {
	s := time.Now().UnixMicro()
	e := s + SimulateWork()

	//add m
	m.AddMeasurement(uint64(100), remoteId, s, e)
	s -= START_EPOCH
	e -= START_EPOCH
	return s, e
}

/***********************************************************************************************************************
 * Test functions
 **********************************************************************************************************************/

func TestNewMeasurement(t *testing.T) {
	listSizeTestParam := 10
	m := CreateMeasurement(listSizeTestParam)
	log.Debugf("New Measurement: \n\tNodeId: %d,\n\tcsvPrefix: %s, \n\tlistSize: %d\n", m.thisNodeId.Int(), m.prefix, len(m.data))

	assert.Equal(t, 0, m.fileCounter, "Measurement structure file counter not initialized to 0")
	assert.Equal(t, 0, len(m.data), "length of initialized list is: %d, exepcted: %d", len(m.data), 0)
	m.Close()
}

func TestEpoch(t *testing.T) {
	correctTimeMicro := time.Date(2023, time.January, 1, 0, 0, 0, 0, time.UTC)
	log.Debugf("Epoch time should be equal to %s = %d\n", correctTimeMicro.String(), correctTimeMicro.UnixMicro())

	assert.Equal(t, START_EPOCH, correctTimeMicro.UnixMicro(), "Epoch is %d but should be %d\n", START_EPOCH, correctTimeMicro.UnixMicro())
}

func TestAddMeasurementOnce(t *testing.T) {
	listSizeTestParam := 10
	fakeNodeId := ids.GetIDFromFlag()
	m := CreateMeasurement(listSizeTestParam)

	s, e := DoMeasurement(m, fakeNodeId)

	entry := m.data[0]
	log.Debugf("Measurement: %d, %d, %d, %d, %d\n", m.thisNodeId.Int(), entry.round, entry.remoteNodeId.Int(), entry.start, entry.end)

	assert.Equal(t, fakeNodeId.Int(), entry.remoteNodeId.Int(), "wrong remote node id recorded")
	assert.Equal(t, s, entry.start, "wrong start time recorded: %d instead of %d", entry.start, s)
	assert.Equal(t, e, entry.end, "wrong end time recorded")

	m.Close()
}

func TestAddMeasurement100(t *testing.T) {
	listSizeTestParam := 101
	fakeNodeId := ids.GetIDFromFlag()
	m := CreateMeasurement(listSizeTestParam)

	for i := 0; i < 100; i++ {
		s, e := DoMeasurement(m, fakeNodeId)

		entry := m.data[i]
		assert.Equal(t, s, entry.start, "wrong start time recorded: %d instead of %d at entry %d\n", entry.start, s, i)
		assert.Equal(t, e, entry.end, "wrong end time recorded: %d instead of %d at entry %d\n", entry.end, e, i)
	}
	m.Close()
}

func TestFlushCSV(t *testing.T) {
	listSizeTestParam := 100
	fakeNodeId := ids.GetIDFromFlag()
	m := CreateMeasurement(listSizeTestParam)

	for i := 0; i < 120; i++ {
		DoMeasurement(m, fakeNodeId)
	}

	assert.Equal(t, 20, len(m.data), "Expected list size %d but got %d\n", 20, len(m.data))
	assert.Equal(t, 1, m.fileCounter, "Expected file counter size %d but fot %d\n", 1, m.fileCounter)

	//sleep to allow time to write file
	time.Sleep(time.Second * 1)

	f, err := os.Open(DIRECTORY + "/" + testPrefix + "_0.csv")
	assert.Equal(t, nil, err, "error: could not find csv file with appropriate name in current directory\n")

	r := csv.NewReader(f)

	//process header
	_, err = r.Read()
	assert.Equal(t, nil, err, "error: empty file\n")

	rows, _ := r.ReadAll()
	assert.Equal(t, listSizeTestParam, len(rows), "error: len of csv file is %d but should be %d\n", len(rows), listSizeTestParam)

	m.Close()
}
