package measurement

//Each client is expected to maintain its own Measurement
//object to update/record data

import (
	"time"

	"github.com/UNH-DistSyS/UNH-CLT/ids"
)

var _startEpoch = time.Date(2023, 0, 0, 0, 0, 0, 0, time.UTC)

type Measurement struct {
	prefix      string
	data        []measurementRow
	fileCounter int32
}

type measurementRow struct {
	round  int64   //round ID
	nodeID *ids.ID //remote node ID that is being measured
	start  int64   //begin time, in microseconds
	end    int64   //end time, in microseconds
}

func (m *Measurement) AddMeasurement(roundNumber int64, remoteNodeID *ids.ID, startTime int64, endTime int64) {
	row := measurementRow{roundNumber, remoteNodeID, startTime, endTime}
	m.data = append(m.data, row)
}

/*
* Measurement should flush data to a file after
a certain threshold is reached
*/
func flush() {

}

func CreateMeasurement(nodeId *ids.ID, csvPrefix string, listSize int32) *Measurement {
	m := &Measurement{
		prefix:      csvPrefix,
		data:        make([]measurementRow, listSize),
		fileCounter: 0,
	}

	return m
}
