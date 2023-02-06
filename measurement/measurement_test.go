package measurement

import (
	"fmt"
	"testing"

	"github.com/UNH-DistSyS/UNH-CLT/ids"
)

func TestCreateMeasurement(t *testing.T) {
	listSizeTestParam := 10

	m := CreateMeasurement(ids.GetIDFromFlag(), "test", listSizeTestParam)

	if m.fileCounter != 0 {
		t.Fatalf("Measurement structure file counter not initialized to 0")
	}
	if len(m.data) != int(listSizeTestParam) {
		t.Fatalf("length of initialized list is: %d, exepcted: %d", len(m.data), listSizeTestParam)
	}
	//view stats
	fmt.Printf("New Measurement: \n\tNodeId: %d,\n\tcsvPrefix: %s, \n\tlistSize: %d\n", m.clientId.Int(), m.prefix, len(m.data))
}

func TestAddMeasurementOnce(t *testing.T) {

}
