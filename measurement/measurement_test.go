package measurement

import (
	"bytes"
	"compress/gzip"
	"encoding/csv"
	"io"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/log"
	"github.com/stretchr/testify/assert"
)

/***********************************************************************************************************************
 * Helper functions/variables
 **********************************************************************************************************************/

var testPrefix = "test"

func CreateMeasurement(listSizeTestParam int) *Measurement {
	return NewMeasurement(ids.GetIDFromFlag(), testPrefix, listSizeTestParam, false)
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
	m.Close()

	assert.Equal(t, 20, len(m.data), "Expected list size %d but got %d\n", 20, len(m.data))
	assert.Equal(t, 2, m.fileCounter, "Expected file counter size %d but fot %d\n", 1, m.fileCounter)

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

func TestFlushGZip(t *testing.T) {
	listSizeTestParam := 100
	fakeNodeId := ids.GetIDFromFlag()
	m := CreateMeasurement(listSizeTestParam)
	m.compress = true

	var all_measures []string

	for i := 0; i < 99; i++ {
		DoMeasurement(m, fakeNodeId)
		all_measures = append(all_measures, m.data[i].String())
	}
	m.Close()

	time.Sleep(time.Second * 1)
	assert.Equal(t, 1, m.fileCounter, "Expected file counter size %d but got %d\n", 1, m.fileCounter)

	f, err := os.Open(DIRECTORY + "/" + testPrefix + "_0.csv.gz")
	assert.Equal(t, nil, err, "error: could not find csv file with appropriate name in current directory\n")
	defer f.Close()

	gz, err := gzip.NewReader(f)
	assert.Equal(t, nil, err, "error: unable to create gzip reader for file %v\n", f)
	defer gz.Close()

	var buf bytes.Buffer

	_, err = io.Copy(&buf, gz)
	assert.Equal(t, nil, err, "error: could not read data from gzip file\n")

	rows := strings.Split(buf.String(), "\n")
	log.Debugf("length all: %v, length uncompressed: %v", len(all_measures), len(rows))

	//compare i:i+1 to ignore header from compressed file
	//splitting on "\n" will also add an empty last element, which we'll ignore
	for i := 1; i < len(rows)-1; i++ {
		assert.Equal(t, all_measures[i-1], rows[i], "measurement and output do not match. got: %v, instead of %v", rows[i], all_measures[i-1])
	}
}

func TestFlushGZipMultiple(t *testing.T) {
	listSizeTestParam := 100
	fakeNodeId := ids.GetIDFromFlag()
	m := CreateMeasurement(listSizeTestParam)
	m.compress = true

	var all_measures []string

	for i := 0; i < 110; i++ {
		index := i % (listSizeTestParam)
		DoMeasurement(m, fakeNodeId)
		all_measures = append(all_measures, m.data[index].String())
	}
	m.Close()

	time.Sleep(time.Second * 1)
	assert.Equal(t, 2, m.fileCounter, "Expected file counter size %d but got %d\n", 1, m.fileCounter)

	f1, err := os.Open(DIRECTORY + "/" + testPrefix + "_0.csv.gz")
	assert.Equal(t, nil, err, "error: could not find csv file with appropriate name in current directory\n")
	defer f1.Close()

	gz, err := gzip.NewReader(f1)
	assert.Equal(t, nil, err, "error: unable to create gzip reader for file %v\n", f1)
	defer gz.Close()

	var buf bytes.Buffer

	_, err = io.Copy(&buf, gz)
	assert.Equal(t, nil, err, "error: could not read data from gzip file\n")

	rows := strings.Split(buf.String(), "\n")

	//compare i:i+1 to ignore header from compressed file
	//splitting on "\n" will also add an empty last element, which we'll ignore
	for i := 1; i < len(rows)-1; i++ {
		assert.Equal(t, all_measures[i-1], rows[i], "measurement and output do not match. got: %v, instead of %v", rows[i], all_measures[i-1])
	}

	f2, err := os.Open(DIRECTORY + "/" + testPrefix + "_1.csv.gz")
	assert.Equal(t, nil, err, "error: could not find csv file with appropriate name in current directory\n")
	defer f1.Close()

	gz, err = gzip.NewReader(f2)
	assert.Equal(t, nil, err, "error: unable to create gzip reader for file %v\n", f2)
	defer gz.Close()

	buf.Reset()
	_, err = io.Copy(&buf, gz)
	assert.Equal(t, nil, err, "error: could not read data from gzip file\n")

	rows = strings.Split(buf.String(), "\n")

	//compare i:i+1 to ignore header from compressed file
	//splitting on "\n" will also add an empty last element, which we'll ignore
	for i := 1; i < len(rows)-1; i++ {
		index := i + listSizeTestParam - 1
		assert.Equal(t, all_measures[index], rows[i], "measurement and output do not match. got: %v, instead of %v", rows[i], all_measures[index])
	}

}
