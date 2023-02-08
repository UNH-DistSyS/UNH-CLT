package measurement

import (
	"encoding/csv"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/UNH-DistSyS/UNH-CLT/ids"
)

/***********************************************************************************************************************
 * Helper functions/variables
 **********************************************************************************************************************/

var DEBUG = true
var testPrefix = "test"

func NewMeasurement(listSizeTestParam int) *Measurement {
	return CreateMeasurement(ids.GetIDFromFlag(), testPrefix, listSizeTestParam)
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
	m.AddMeasurement(int64(100), remoteId, s, e)
	s -= _startEpoch
	e -= _startEpoch
	return s, e
}

/***********************************************************************************************************************
 * Test functions
 **********************************************************************************************************************/

func TestCreateMeasurement(t *testing.T) {
	listSizeTestParam := 10
	m := NewMeasurement(listSizeTestParam)

	//view stats
	if DEBUG {
		fmt.Printf("New Measurement: \n\tNodeId: %d,\n\tcsvPrefix: %s, \n\tlistSize: %d\n", m.thisNodeId.Int(), m.prefix, len(m.data))
	}

	if m.fileCounter != 0 {
		t.Fatalf("Measurement structure file counter not initialized to 0")
	}
	if len(m.data) != 0 {
		t.Fatalf("length of initialized list is: %d, exepcted: %d", len(m.data), 0)
	}
}

func TestEpoch(t *testing.T) {
	correctTimeMicro := time.Date(2023, time.January, 1, 0, 0, 0, 0, time.UTC)
	if DEBUG {
		fmt.Printf("Epoch time will be equal to %s\n", correctTimeMicro.String())
	}

	if correctTimeMicro.UnixMicro() != _startEpoch {
		t.Fatalf("Epoch is %d but should be %d\n", _startEpoch, correctTimeMicro.UnixMicro())
	}
}

// IN PROGRESS
func TestAddMeasurementOnce(t *testing.T) {
	listSizeTestParam := 10
	fakeNodeId := ids.GetIDFromFlag()
	m := NewMeasurement(listSizeTestParam)

	s, e := DoMeasurement(m, fakeNodeId)

	entry := m.data[0]
	if DEBUG {
		fmt.Printf("Measurement: %d, %d, %d, %d, %d\n", m.thisNodeId.Int(), entry.round, entry.remoteNodeId.Int(), entry.start, entry.end)
	}

	if entry.remoteNodeId.Int() != fakeNodeId.Int() {
		t.Fatalf("wrong remote node id recorded")
	}
	if entry.start != s {
		t.Fatalf("wrong start time recorded: %d instead of %d", entry.start, s)
	}
	if entry.end != e {
		t.Fatalf("wrong end time recorded")
	}

	//if m.data[0]
}

func TestAddMeasurement100(t *testing.T) {
	listSizeTestParam := 101
	fakeNodeId := ids.GetIDFromFlag()
	m := NewMeasurement(listSizeTestParam)

	for i := 0; i < 100; i++ {
		s, e := DoMeasurement(m, fakeNodeId)

		entry := m.data[i]
		if entry.start != s {
			t.Fatalf("wrong start time recorded: %d instead of %d at entry %d\n", entry.start, s, i)
		}
		if entry.end != e {
			t.Fatalf("wrong end time recorded: %d instead of %d at entry %d\n", entry.end, e, i)
		}

	}
}

func TestFlushCSV(t *testing.T) {
	listSizeTestParam := 100
	fakeNodeId := ids.GetIDFromFlag()
	m := NewMeasurement(listSizeTestParam)

	for i := 0; i < 120; i++ {
		DoMeasurement(m, fakeNodeId)
	}

	if len(m.data) != 20 {
		t.Fatalf("Expected list size %d but got %d\n", 20, len(m.data))
	}
	if m.fileCounter != 1 {
		t.Fatalf("Expected file counter size %d but fot %d\n", 1, m.fileCounter)
	}

	//sleep to allow time to write file
	time.Sleep(time.Second * 2)
	f, err := os.Open(testPrefix + "_0.csv")
	if err != nil {
		t.Fatalf("error: could not find csv file with appropriate name in current directory\n")
	}
	r := csv.NewReader(f)
	//process header
	if _, err := r.Read(); err != nil {
		t.Fatalf("error: empty file\n")
	}

	rows, _ := r.ReadAll()
	if len(rows) != listSizeTestParam {
		t.Fatalf("error: len of csv file is %d but should be %d\n", len(rows), listSizeTestParam)
	}
}
