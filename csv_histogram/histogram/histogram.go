package histogram

import (
	"encoding/csv"
	"math"
	"os"
	"sort"
	"strconv"
	"sync"
)

const HIST_BUCKET_SIZE = 100

type Histogram struct {
	hist                 map[int]int
	histogramBucketWidth int

	sync.RWMutex
}

func NewHistogram(defaultSize, bucketWidthNs int) *Histogram {
	latencyHistogramNs := make(map[int]int, defaultSize)
	for b := 0; b < defaultSize; b++ {
		latencyHistogramNs[b] = 0
	}

	return &Histogram{
		hist:                 latencyHistogramNs,
		histogramBucketWidth: bucketWidthNs,
	}

}

func (h *Histogram) Add(measurement int) {
	// TODO lock map?
	h.Lock()
	defer h.Unlock()
	h.hist[int(measurement)/h.histogramBucketWidth] += 1
}

func (h *Histogram) Count() int {
	h.RLock()
	defer h.RUnlock()
	count := 0
	for _, c := range h.hist {
		count += c
	}

	return count
}

func (h *Histogram) Mean() int {
	h.RLock()
	defer h.RUnlock()
	count := 0
	sum := 0
	for i, c := range h.hist {
		count += c
		sum += c * (i*h.histogramBucketWidth + h.histogramBucketWidth/2)
	}
	return sum / count
}

func (h *Histogram) Variance() float64 {
	mean := h.Mean()
	count := h.Count()
	h.RLock()
	defer h.RUnlock()
	sum := 0
	for i, c := range h.hist {
		val := i*h.histogramBucketWidth + h.histogramBucketWidth/2
		diff := (val - mean)
		diffSq := diff * diff
		sum += c * diffSq
	}

	variance := float64(sum) / float64(count-1)

	return variance
}

func (h *Histogram) StdDev() float64 {
	variance := h.Variance()
	stddev := math.Sqrt(variance)
	return stddev
}

func (h *Histogram) StdErr() float64 {
	stddev := h.StdDev()
	count := h.Count()
	h.RLock()
	defer h.RUnlock()
	stderr := stddev / math.Sqrt(float64(count))

	return stderr
}

func (h *Histogram) ApproxPercentile(p float64) int {
	count := h.Count()
	h.RLock()
	defer h.RUnlock()
	percentileCount := int(float64(count) * p)

	buckets := make([]int, 0, len(h.hist))

	for b, _ := range h.hist {
		buckets = append(buckets, b)
	}

	sort.Ints(buckets)

	c := 0
	for i := 0; i < len(buckets); i++ {
		if c >= percentileCount {
			return buckets[i] * h.histogramBucketWidth
		}
		c += h.hist[buckets[i]]
	}

	return buckets[len(buckets)-1] * h.histogramBucketWidth
}

func (h *Histogram) Max() int {
	h.RLock()
	defer h.RUnlock()
	maxBucket := 0
	for i, c := range h.hist {
		if i > maxBucket && c > 0 {
			maxBucket = i
		}
	}
	return (maxBucket*h.histogramBucketWidth + h.histogramBucketWidth/2)
}

func (h *Histogram) WriteToCSV(filename string) error {
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
	headers := []string{"bucket number (bucket width=" + strconv.Itoa(h.histogramBucketWidth) + " microseconds)", "count"}
	err = writer.Write(headers)
	if err != nil {
		return err
	}

	maxBucket := 0
	for i, c := range h.hist {
		if i > maxBucket && c > 0 {
			maxBucket = i
		}
	}

	// sort the keys
	keys := make([]int, 0, len(h.hist))
	for k := range h.hist {
		if k <= maxBucket {
			keys = append(keys, k)
		}
	}
	sort.Ints(keys)

	// write the data
	for _, key := range keys {
		value := h.hist[key]
		row := []string{strconv.Itoa(key), strconv.Itoa(value)}
		writer.Write(row)
	}

	return nil
}

func (h *Histogram) GetHistogram() map[int]int {

	truncatedhist := make(map[int]int)
	maxBucket := 0
	for i, c := range h.hist {
		if i > maxBucket && c > 0 {
			maxBucket = i
		}
	}

	// sort the keys
	keys := make([]int, 0, len(h.hist))
	for k := range h.hist {
		if k <= maxBucket {
			keys = append(keys, k)
		}
	}
	sort.Ints(keys)

	// write the data
	for _, key := range keys {
		value := h.hist[key]
		truncatedhist[key] = value
	}

	return truncatedhist
}
