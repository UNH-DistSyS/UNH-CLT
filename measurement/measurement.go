package measurement

//Each client is expected to maintain its own Measurement
//object to update/record data

import (
	"bytes"
	"compress/gzip"
	"encoding/csv"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/log"
)

// local Epoch equal to January 1, 2023 00:00:00 UTC
const (
	START_EPOCH int64  = 1672531200000000
	DIRECTORY   string = "raw_data"
)

type Measurement struct {
	thisNodeId *ids.ID
	prefix     string
	data       []measurementRow
	compress   bool

	//internal variables for tracking/updating
	mu          sync.Mutex
	listLength  int
	fileCounter int
}

type measurementRow struct {
	thisNodeId   *ids.ID //id of node using Measurement
	round        uint64  //round ID
	remoteNodeId *ids.ID //remote node ID that is being measured
	start        int64   //begin time, in microseconds
	end          int64   //end time, in microseconds
}

/* When nodes stop, they must call this function to flush remaining data to file */
func (m *Measurement) Close() {
	flush(m.data, m.prefix, m.fileCounter, m.compress)
	m.fileCounter++
}

func (m *Measurement) AddMeasurement(roundNumber uint64, remoteNodeID *ids.ID, startTime int64, endTime int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	//flush list to csv if full
	if len(m.data) == m.listLength {
		go flush(m.data, m.prefix, m.fileCounter, m.compress)

		//reset internal list and update variables
		m.fileCounter++
		m.data = make([]measurementRow, 0, m.listLength)
	}
	modifiedStart := startTime - START_EPOCH
	modifiedEnd := endTime - START_EPOCH

	row := measurementRow{
		thisNodeId:   m.thisNodeId,
		round:        roundNumber,
		remoteNodeId: remoteNodeID,
		start:        modifiedStart,
		end:          modifiedEnd,
	}
	m.data = append(m.data, row)
}

/*
* Measurement should flush data to a file after
* a certain threshold is reached.
 */
func flush(data []measurementRow, prefix string, counter int, compress bool) {
	log.Debugf("Flushing output")
	err := os.MkdirAll(DIRECTORY, os.ModePerm) //assert directory exists or creates one
	if err != nil {
		log.Fatalf("Error creating/checking for directory %v\n", DIRECTORY)
	}
	var fileName string
	if compress {
		fileName = DIRECTORY + "/" + prefix + "_" + strconv.Itoa(counter) + ".csv.gz"
	} else {
		fileName = DIRECTORY + "/" + prefix + "_" + strconv.Itoa(counter) + ".csv"
	}
	file, err := os.Create(fileName)
	if err != nil {
		log.Fatalf("Failed to create file %s", fileName)
	}
	defer file.Close()

	if compress {
		gz := gzip.NewWriter(file)
		WriteGZip(gz, data)
		gz.Close()
	} else {
		w := csv.NewWriter(file)
		WriteCSV(w, data)
	}
}

func WriteGZip(gzWriter io.Writer, data []measurementRow) {
	var content []byte = make([]byte, 0)
	header := []byte(strings.Join([]string{"thisNodeId", "roundNumber", "remoteNodeId", "startTime", "endTime"}, ",") + "\n")
	content = append(content, header...)
	log.Debugf("%v", header)

	for _, item := range data {
		row := []byte(item.String() + "\n")
		content = append(content, row...)
	}
	_, err := io.Copy(gzWriter, bytes.NewReader(content))
	if err != nil {
		log.Debugf("Error outputting to file")
	}
}

func WriteCSV(csvWriter *csv.Writer, data []measurementRow) {
	csvWriter.Write([]string{"thisNodeId", "roundNumber", "remoteNodeId", "startTime", "endTime"})
	for _, item := range data {
		row := []string{
			item.thisNodeId.String(),
			strconv.FormatUint(item.round, 10),
			item.remoteNodeId.String(),
			strconv.FormatInt(item.start, 10),
			strconv.FormatInt(item.end, 10),
		}
		err := csvWriter.Write(row)
		if err != nil {
			log.Debugf("Error outputting to file")
		}
	}
	csvWriter.Flush()
}

func (mr *measurementRow) String() string {
	return strings.Join([]string{
		mr.thisNodeId.String(),
		strconv.FormatUint(mr.round, 10),
		mr.remoteNodeId.String(),
		strconv.FormatInt(mr.start, 10),
		strconv.FormatInt(mr.end, 10),
	}, ",")
}

func NewMeasurement(nodeId *ids.ID, csvPrefix string, listSize int, compression bool) *Measurement {
	m := &Measurement{
		thisNodeId:  nodeId,
		prefix:      csvPrefix,
		data:        make([]measurementRow, 0, listSize),
		compress:    compression,
		listLength:  listSize,
		fileCounter: 0,
	}

	return m
}
