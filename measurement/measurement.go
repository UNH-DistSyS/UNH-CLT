package measurement

//Each client is expected to maintain its own Measurement
//object to update/record writeup_figures

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
	mu               sync.Mutex
	memoryListLength int
	CSVListLength    int
	fileCounter      int
	flushLock        sync.Mutex
}

type measurementRow struct {
	thisNodeId   ids.ID //id of node using Measurement
	round        uint32 //round ID
	remoteNodeId ids.ID //remote node ID that is being measured
	start        int64  //begin time, in microseconds
	end          int64  //end time, in microseconds
}

/* When nodes stop, they must call this function to flush remaining writeup_figures to file */
func (m *Measurement) Close() {
	m.flush(m.data)
}

func (m *Measurement) AddMeasurement(roundNumber uint32, remoteNodeID ids.ID, startTime int64, endTime int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if int(roundNumber)%m.CSVListLength == 0 {
		log.Infof("Adding measurement for round %d", roundNumber)
	}

	//flush list to csv if full
	if len(m.data) == m.memoryListLength {
		go m.flush(m.data)

		//reset internal list and update variables
		m.data = make([]measurementRow, 0, m.memoryListLength)
	}
	modifiedStart := startTime - START_EPOCH
	modifiedEnd := endTime - START_EPOCH

	row := measurementRow{
		thisNodeId:   *m.thisNodeId,
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
func (m *Measurement) flush(data []measurementRow) {
	m.flushLock.Lock()
	defer m.flushLock.Unlock()
	log.Debugf("Flushing output")
	err := os.MkdirAll(DIRECTORY, os.ModePerm) //assert directory exists or creates one
	if err != nil {
		log.Fatalf("Error creating/checking for directory %v\n", DIRECTORY)
	}

	lastOffset := 0

	for lastOffset < len(data) {
		newOffset := lastOffset + m.CSVListLength
		if newOffset > len(data) {
			newOffset = len(data)
		}
		dt := data[lastOffset:newOffset]
		lastOffset = newOffset

		var fileName string
		if m.compress {
			fileName = DIRECTORY + "/" + m.prefix + "_" + strconv.Itoa(m.fileCounter) + ".csv.gz"
		} else {
			fileName = DIRECTORY + "/" + m.prefix + "_" + strconv.Itoa(m.fileCounter) + ".csv"
		}
		file, err := os.Create(fileName)
		if err != nil {
			log.Fatalf("Failed to create file %s: %v", fileName, err)
		}

		if m.compress {
			gz := gzip.NewWriter(file)
			WriteGZip(gz, dt)
			gz.Close()
		} else {
			w := csv.NewWriter(file)
			WriteCSV(w, dt)
		}

		m.fileCounter += 1
		file.Close()
	}
}

func WriteGZip(gzWriter io.Writer, data []measurementRow) {
	var buf bytes.Buffer
	header := []byte(strings.Join([]string{"thisNodeId", "roundNumber", "remoteNodeId", "startTime", "endTime"}, ",") + "\n")
	buf.Write(header)
	for _, item := range data {
		row := []byte(item.String() + "\n")
		buf.Write(row)
	}
	_, err := io.Copy(gzWriter, &buf)
	if err != nil {
		log.Debugf("Error outputting to file")
	}
}

func WriteCSV(csvWriter *csv.Writer, data []measurementRow) {
	csvWriter.Write([]string{"thisNodeId", "roundNumber", "remoteNodeId", "startTime", "endTime"})
	for _, item := range data {
		row := []string{
			item.thisNodeId.String(),
			strconv.FormatUint(uint64(item.round), 10),
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
		strconv.FormatUint(uint64(mr.round), 10),
		mr.remoteNodeId.String(),
		strconv.FormatInt(mr.start, 10),
		strconv.FormatInt(mr.end, 10),
	}, ",")
}

func NewMeasurement(nodeId *ids.ID, csvPrefix string, memoryListSize, csvListSize int, compression bool) *Measurement {
	m := &Measurement{
		thisNodeId:       nodeId,
		prefix:           csvPrefix,
		data:             make([]measurementRow, 0, memoryListSize),
		compress:         compression,
		memoryListLength: memoryListSize,
		CSVListLength:    csvListSize,
		fileCounter:      0,
	}

	return m
}
