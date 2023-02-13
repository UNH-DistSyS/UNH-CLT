package measurement

//Each client is expected to maintain its own Measurement
//object to update/record data

import (
	"encoding/csv"
	"log"
	"os"
	"strconv"
	"sync"

	"github.com/UNH-DistSyS/UNH-CLT/ids"
)

// local Epoch equal to January 1, 2023 00:00:00 UTC
const _startEpoch = 1672531200000000

type Measurement struct {
	thisNodeId *ids.ID
	prefix     string
	data       []measurementRow

	//internal variables for tracking/updating
	mu          sync.Mutex
	listLength  int
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
		thisNodeId:   m.thisNodeId,
		round:        roundNumber,
		remoteNodeId: remoteNodeID,
		start:        modifiedStart,
		end:          modifiedEnd,
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = append(m.data, row)

	//flush list to csv if full
	if len(m.data) == m.listLength {
		go flush(m.data, m.prefix, m.fileCounter)

		//reset internal list and update variables
		m.fileCounter++
		m.data = make([]measurementRow, 0, m.listLength)
	}
}

/*
* Measurement should flush data to a file after
* a certain threshold is reached.
 */
func flush(data []measurementRow, prefix string, counter int) {
	fileName := prefix + "_" + strconv.Itoa(counter) + ".csv"
	file, err := os.Create(fileName)
	defer file.Close()
	if err != nil {
		log.Fatalf("Failed to create file %s", fileName)
	}

	w := csv.NewWriter(file)

	//write header
	w.Write([]string{"thisNodeId", "roundNumber", "remoteNodeId", "startTime", "endTime"})

	for _, item := range data {
		w.Write([]string{
			item.thisNodeId.String(),
			strconv.FormatInt(item.round, 10),
			item.remoteNodeId.String(),
			strconv.FormatInt(item.start, 10),
			strconv.FormatInt(item.end, 10),
		})
	}
	w.Flush()
}

func NewMeasurement(nodeId *ids.ID, csvPrefix string, listSize int) *Measurement {
	m := &Measurement{
		thisNodeId:  nodeId,
		prefix:      csvPrefix,
		data:        make([]measurementRow, 0, listSize),
		listLength:  listSize,
		fileCounter: 0,
	}

	return m
}
