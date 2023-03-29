package main

import (
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/UNH-DistSyS/UNH-CLT/csv_histogram/histogram"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

var csvdir = flag.String("csvdir", "", "location of csv files")
var outdir = flag.String("outdir", "", "location of output csv files")
var experimentsJson = flag.String("experiments_json", "", "location of JSON file with experimental description")
var bucketWidth = flag.Int("bucket", 5, "microseconds in each histogram bucket")
var crateHistogramImages = flag.Bool("images", false, "whether to generate histogram images")

type Bucket struct {
	Label       string     `json:"label"`
	NodePairs   [][]string `json:"node_pairs"`
	QuorumSizes []int      `json:"quorum_sizes"`
}

type QuorumDescription struct {
	StartNode string
	EndNodes  []string
	Size      int

	roundLatencies map[int][]int // round -> list of latencies
	Histogram      *histogram.Histogram
}

func newQuorumDescription(startNode string, size int, h *histogram.Histogram) *QuorumDescription {
	return &QuorumDescription{
		StartNode:      startNode,
		EndNodes:       make([]string, 0),
		Size:           size,
		roundLatencies: make(map[int][]int, 0),
		Histogram:      h,
	}
}

func (qd *QuorumDescription) isValidEndNode(id string) bool {
	for _, endNodeId := range qd.EndNodes {
		if endNodeId == id {
			return true
		}
	}
	return false
}

func (qd *QuorumDescription) AddQuorumLatency(latency, round int, nodeId string) {
	if qd.isValidEndNode(nodeId) {
		if _, exists := qd.roundLatencies[round]; exists {
			qd.roundLatencies[round] = append(qd.roundLatencies[round], latency)

			if len(qd.roundLatencies[round]) == len(qd.EndNodes) {
				sort.Ints(qd.roundLatencies[round])
				qd.Histogram.Add(qd.roundLatencies[round][qd.Size-1])
				delete(qd.roundLatencies, round)
			}
		} else {
			qd.roundLatencies[round] = make([]int, 0)
			qd.roundLatencies[round] = append(qd.roundLatencies[round], latency)
		}
	}
}

func main() {
	flag.Parse()
	buckets, err := parseJSON(*experimentsJson)
	if err != nil {
		fmt.Println("Error parsing JSON:", err)
		return
	}
	histograms := make([]*histogram.Histogram, 0)
	pairsToHistograms := make(map[string][]*histogram.Histogram)
	quorumDescriptions := make(map[string][]*QuorumDescription)
	for i, bucket := range buckets {
		fmt.Println(bucket.Label)
		fmt.Println(bucket.NodePairs)

		if bucket.QuorumSizes == nil {
			h := histogram.NewHistogram(1e4, *bucketWidth)
			histograms = append(histograms, h)
			for _, nodePair := range bucket.NodePairs {
				npStr := nodePair[0] + nodePair[1]
				if hist, exists := pairsToHistograms[npStr]; exists {
					hist = append(hist, histograms[i])
				} else {
					pairsToHistograms[npStr] = make([]*histogram.Histogram, 0)
					pairsToHistograms[npStr] = append(pairsToHistograms[npStr], h)
				}
			}
		} else {
			fmt.Println("Quorum Mode")
			for _, qs := range bucket.QuorumSizes {
				h := histogram.NewHistogram(1e4, *bucketWidth)
				histograms = append(histograms, h)
				for _, nodePair := range bucket.NodePairs {
					if qds, exists := quorumDescriptions[nodePair[0]]; exists {
						nodeAdded := false
						for _, qd := range qds {
							if qd.Size == qs {
								qd.EndNodes = append(qd.EndNodes, nodePair[1])
								nodeAdded = true
							}
						}
						if !nodeAdded {
							qd := newQuorumDescription(nodePair[0], qs, h)
							qd.EndNodes = append(qd.EndNodes, nodePair[1])
							quorumDescriptions[nodePair[0]] = append(quorumDescriptions[nodePair[0]], qd)
						}
					} else {
						quorumDescriptions[nodePair[0]] = make([]*QuorumDescription, 0)
						qd := newQuorumDescription(nodePair[0], qs, h)
						qd.EndNodes = append(qd.EndNodes, nodePair[1])
						quorumDescriptions[nodePair[0]] = append(quorumDescriptions[nodePair[0]], qd)
					}
				}
			}
		}
	}

	fmt.Println(pairsToHistograms)

	_, err = parseCSVFiles(*csvdir, pairsToHistograms, quorumDescriptions)
	if err != nil {
		fmt.Println("Error parsing CSV:", err)
		return
	}

	i := 0
	for _, bucket := range buckets {
		numHistograms := 0
		if bucket.QuorumSizes == nil {
			numHistograms = 1
		} else {
			numHistograms = len(bucket.QuorumSizes)
		}

		for j := 0; j < numHistograms; j++ {
			lbl := strings.ReplaceAll(bucket.Label, " ", "_")
			fmt.Println("------------------------------------")
			fmt.Println("Experiment:", bucket.Label)
			fmt.Println("------------------------------------")
			if bucket.QuorumSizes != nil {
				fmt.Printf("Quourm of %d nodes\n", bucket.QuorumSizes[j])
				lbl = lbl + "_quorum" + strconv.Itoa(bucket.QuorumSizes[j])
			}
			fmt.Printf("Number of Observations: %d\n", histograms[i].Count())
			fmt.Printf("Average Latency: %f ms\n", float64(histograms[i].Mean())/1000)
			fmt.Printf("Variance: %f ms\n", histograms[i].Variance()/1000)
			fmt.Printf("Std. Dev: %f ms\n", histograms[i].StdDev()/1000)
			fmt.Printf("Std. Err: %f ms\n", histograms[i].StdErr()/1000)
			fmt.Printf("25th Percentile Latency: %f ms\n", float64(histograms[i].ApproxPercentile(0.25))/1000)
			fmt.Printf("Median Latency: %f ms\n", float64(histograms[i].ApproxPercentile(0.5))/1000)
			fmt.Printf("75th Percentile Latency: %f ms\n", float64(histograms[i].ApproxPercentile(0.75))/1000)
			fmt.Printf("90th Percentile Latency: %f ms\n", float64(histograms[i].ApproxPercentile(0.90))/1000)
			fmt.Printf("95th Percentile Latency: %f ms\n", float64(histograms[i].ApproxPercentile(0.95))/1000)
			fmt.Printf("99th Percentile Latency: %f ms\n", float64(histograms[i].ApproxPercentile(0.99))/1000)
			fmt.Printf("99.9th Percentile Latency: %f ms\n", float64(histograms[i].ApproxPercentile(0.999))/1000)
			fmt.Printf("99.99th Percentile Latency: %f ms\n", float64(histograms[i].ApproxPercentile(0.9999))/1000)
			fmt.Printf("99.999th Percentile Latency: %f ms\n", float64(histograms[i].ApproxPercentile(0.99999))/1000)
			fmt.Printf("Max Latency: %f ms\n", float64(histograms[i].Max())/1000)
			fmt.Println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

			if *outdir != "" {
				err = histograms[i].WriteToCSV(*outdir + "/histogram_" + lbl + ".csv")
				if err != nil {
					fmt.Println("Error writing CSV", err)
					return
				}

				if *crateHistogramImages {
					histogram.PlotHistogram(histograms[i], *outdir+"/histogram_"+lbl+".png", bucket.Label, true, 0)
					histogram.PlotHistogram(histograms[i], *outdir+"/histogram_"+lbl+"_tail.png", bucket.Label, true, 1000)
				}
			}
			i += 1
		}
	}
}

func parseCSVFiles(directoryPath string, pairsToHistograms map[string][]*histogram.Histogram, quorumDescriptions map[string][]*QuorumDescription) ([]string, error) {
	var filenames []string

	// Walk the directory and get all CSV file names
	err := filepath.Walk(directoryPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if strings.HasSuffix(path, ".csv") || strings.HasSuffix(path, ".csv.gz") {
			filenames = append(filenames, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Parse all CSV files
	for _, filename := range filenames {
		fmt.Println("Working on", filename)
		var file *os.File
		var reader *csv.Reader

		if strings.HasSuffix(filename, ".gz") {
			// If the file is gzipped, decompress it first
			file, err = os.Open(filename)
			if err != nil {
				return nil, err
			}
			defer file.Close()

			gzipReader, err := gzip.NewReader(file)
			if err != nil {
				return nil, err
			}
			defer gzipReader.Close()

			reader = csv.NewReader(gzipReader)
		} else {
			// If the file is not gzipped, just open it normally
			file, err = os.Open(filename)
			if err != nil {
				return nil, err
			}
			defer file.Close()

			reader = csv.NewReader(file)
		}

		for {
			record, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}

			if len(record) != 5 {
				continue
			}
			// Do something with the CSV record, e.g. print it out
			//fmt.Println(record)
			nodePairStr := record[0] + record[2]

			startTime, err := strconv.Atoi(record[3])
			if err != nil {
				continue
			}
			endTime, err := strconv.Atoi(record[4])
			if err != nil {
				continue
			}

			latency := endTime - startTime

			for _, hist := range pairsToHistograms[nodePairStr] {
				hist.Add(latency)
			}

			/*round, err := strconv.Atoi(record[1])
			if err != nil {
				continue
			}*/

			if qds, exists := quorumDescriptions[record[0]]; exists {
				round, err := strconv.Atoi(record[1])
				if err != nil {
					continue
				}

				for _, qd := range qds {
					qd.AddQuorumLatency(latency, round, record[2])
				}
			}

		}
	}

	return filenames, nil
}

func parseJSON(filename string) ([]Bucket, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var result struct {
		Buckets []Bucket `json:"buckets"`
	}

	decoder := json.NewDecoder(file)
	err = decoder.Decode(&result)
	if err != nil {
		return nil, err
	}

	return result.Buckets, nil
}
