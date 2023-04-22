package data_processing

import (
	"encoding/csv"
	"os"
	"sort"
	"strconv"
	"sync"
)

const WINDOW_SIZE = 100

type windowStat struct {
	sum   int
	count int
}

type WindowAggregator struct {
	windows       map[int]*windowStat
	windowWidthMs int

	sync.RWMutex
}

func NewWindowAggregator(windowWidthMs int) *WindowAggregator {

	return &WindowAggregator{
		windows:       make(map[int]*windowStat),
		windowWidthMs: windowWidthMs,
	}
}

func (w *WindowAggregator) GetWindowWidth() int {
	return w.windowWidthMs
}

func (w *WindowAggregator) AdjustForEpochTime(et int) {
	windTemp := w.windows
	w.windows = make(map[int]*windowStat)

	etBucket := et / w.windowWidthMs

	for t, ws := range windTemp {
		w.windows[t-etBucket] = ws
	}
}

func (w *WindowAggregator) Add(startTimeMs, measurement int) {
	w.Lock()
	defer w.Unlock()

	bucket := startTimeMs / w.windowWidthMs
	ws := w.windows[bucket]
	if ws == nil {
		ws := &windowStat{
			sum:   measurement,
			count: 1,
		}
		w.windows[bucket] = ws
	} else {
		ws.sum += measurement
		ws.count += 1
	}
}

func (w *WindowAggregator) WriteToCSV(filename string) error {
	w.RLock()
	defer w.RUnlock()
	// create the CSV file
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// create a CSV writer
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// write the headers
	headers := []string{"elapsed time (ms)", "average latency"}
	err = writer.Write(headers)
	if err != nil {
		return err
	}

	// sort the keys
	keys := make([]int, 0, len(w.windows))
	for k := range w.windows {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	// write the data
	for _, key := range keys {
		value := w.windows[key]
		row := []string{strconv.Itoa(key * w.windowWidthMs), strconv.Itoa(value.sum / value.count)}
		writer.Write(row)
	}

	return nil
}

func (w *WindowAggregator) GetAverageAggregates() map[int]int {
	avAggregates := make(map[int]int)

	// sort the keys
	keys := make([]int, 0, len(w.windows))
	for k, c := range w.windows {
		if c.count > 0 {
			keys = append(keys, k)
		}
	}
	sort.Ints(keys)

	// write the data
	for _, key := range keys {
		value := w.windows[key]
		avAggregates[key] = value.sum / value.count
	}

	return avAggregates
}
