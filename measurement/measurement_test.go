package measurement

import (
	"fmt"
	"testing"

	"github.com/UNH-DistSyS/UNH-CLT/ids"
)

func NewMeasurement(listSizeTestParam int) *Measurement {
	m := CreateMeasurement(ids.GetIDFromFlag(), "test", listSizeTestParam)
	//view stats
	fmt.Printf("New Measurement: \n\tNodeId: %d,\n\tcsvPrefix: %s, \n\tlistSize: %d\n", m.clientId.Int(), m.prefix, len(m.data))
	return m
}

func TestCreateMeasurement(t *testing.T) {
	listSizeTestParam := 10
	m := NewMeasurement(listSizeTestParam)

	if m.fileCounter != 0 {
		t.Fatalf("Measurement structure file counter not initialized to 0")
	}
	if len(m.data) != int(listSizeTestParam) {
		t.Fatalf("length of initialized list is: %d, exepcted: %d", len(m.data), listSizeTestParam)
	}
}

func TestEpoch(t *testing.T) {
	//TODO: test that epoch is accurate to Jan 1 2023 0:00:00
}

func TestAddMeasurementOnce(t *testing.T) {
	//listSizeTestParam := 10
	//fakeNodeId := ids.GetIDFromFlag()
	//m := NewMeasurement(listSizeTestParam)

	//IN PROGRESS
	//m.AddMeasurement(int64(100), &fakeNodeId, )

	//if m.data[0]
}
