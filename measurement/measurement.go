package measurement

//Each client is expected to maintain its own Measurement
//object to update/record data

import (
	"encoding/csv"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/UNH-DistSyS/UNH-CLT/ids"
)

var _startEpoch = (time.Date(2023, time.January, 1, 0, 0, 0, 0, time.UTC)).UnixMicro()

type Measurement struct {
	clientId *ids.ID
	prefix   string
	data     []measurementRow

	//internal variables for tracking
	listLength  int
	rowCounter  int
	fileCounter int
}

type measurementRow struct {
	thisNodeId   *ids.ID //id of node using Measurement
	round        int64   //round ID
	remoteNodeId *ids.ID //remote node ID that is being measured
	start        int64   //begin time, in microseconds
	end          int64   //end time, in microseconds
}

func (m *Measurement) AddMeasurement(roundNumber int64, remoteNodeID *ids.ID, startTime int64, endTime int64) {
	modifiedStart := startTime - _startEpoch
	modifiedEnd := endTime - _startEpoch

	row := measurementRow{
		thisNodeId:   m.clientId,
		round:        roundNumber,
		remoteNodeId: remoteNodeID,
		start:        modifiedStart,
		end:          modifiedEnd,
	}
	m.data[m.rowCounter] = row
	m.rowCounter++

	//flush list to csv if full
	if m.rowCounter == m.listLength {
		go flush(m.data, m.prefix, m.fileCounter)

		//reset internal list and update variables
		m.rowCounter = 0
		m.fileCounter++
		m.data = make([]measurementRow, m.listLength)
	}
}

/*
* Measurement should flush data to a file after
a certain threshold is reached.
*/
func flush(data []measurementRow, prefix string, counter int) {
	fileName := prefix + "_" + strconv.Itoa(counter) + ".csv"
	file, err := os.Create(fileName)
	defer file.Close()
	if err != nil {
		//TODO: decide output
		log.Fatalf("Failed to create/open file %s", fileName)
	}

	w := csv.NewWriter(file)
	for _, item := range data {
		w.Write([]string{item.thisNodeId.String(), strconv.FormatInt(item.round, 10), item.remoteNodeId.String(), strconv.FormatInt(item.start, 10), strconv.FormatInt(item.end, 10)})
	}
	w.Flush()
}

func CreateMeasurement(nodeId *ids.ID, csvPrefix string, listSize int) *Measurement {
	m := &Measurement{
		clientId:    nodeId,
		prefix:      csvPrefix,
		data:        make([]measurementRow, listSize),
		listLength:  listSize,
		rowCounter:  0,
		fileCounter: 0,
	}

	return m
}
