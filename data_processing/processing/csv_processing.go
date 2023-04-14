package main

import (
	"compress/gzip"
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/UNH-DistSyS/UNH-CLT/data_processing"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

var csvdir = flag.String("csvdir", "", "location of csv files")
var outdir = flag.String("outdir", "", "location of output csv files")
var experimentsJson = flag.String("experiments_json", "", "location of JSON file with experimental description")
var histogramBucketWidth = flag.Int("hb", 5, "microseconds in each histogram bucket")
var windowWidth = flag.Int("ww", 1000, "milliseconds in each aggregated latency window")
var crateImages = flag.Bool("images", false, "whether to generate histogram images")
var trimRawData = flag.Int("trim", 20, "How many rounds to discard at the beginning and end of each data file")

func main() {
	flag.Parse()
	buckets, err := data_processing.ParseJSON(*experimentsJson)
	if err != nil {
		fmt.Println("Error parsing JSON:", err)
		return
	}
	windowedAggregators := make([]*data_processing.WindowAggregator, 0)
	histograms := make([]*data_processing.Histogram, 0)
	pairsToHistograms := make(map[string][]*data_processing.Histogram)
	pairsToWindowAggregators := make(map[string][]*data_processing.WindowAggregator)
	quorumDescriptions := make(map[string][]*data_processing.QuorumDescription)
	for i, bucket := range buckets {
		fmt.Println(bucket.Label)
		fmt.Println(bucket.NodePairs)

		if bucket.QuorumSizes == nil {
			if bucket.DoHistogram {
				h := data_processing.NewHistogram(1e4, *histogramBucketWidth)
				histograms = append(histograms, h)
				for _, nodePair := range bucket.NodePairs {
					npStr := nodePair[0] + nodePair[1]
					if hist, exists := pairsToHistograms[npStr]; exists {
						hist = append(hist, histograms[i])
					} else {
						pairsToHistograms[npStr] = make([]*data_processing.Histogram, 0)
						pairsToHistograms[npStr] = append(pairsToHistograms[npStr], h)
					}
				}
			}

			if bucket.DoWindowedLatencyAggregation {
				w := data_processing.NewWindowAggregator(*windowWidth)
				windowedAggregators = append(windowedAggregators, w)
				for _, nodePair := range bucket.NodePairs {
					npStr := nodePair[0] + nodePair[1]
					if wa, exists := pairsToWindowAggregators[npStr]; exists {
						wa = append(wa, windowedAggregators[i])
					} else {
						pairsToWindowAggregators[npStr] = make([]*data_processing.WindowAggregator, 0)
						pairsToWindowAggregators[npStr] = append(pairsToWindowAggregators[npStr], w)
					}
				}
			}
		} else {
			fmt.Println("Quorum Mode")
			for _, qs := range bucket.QuorumSizes {
				h := data_processing.NewHistogram(1e4, *histogramBucketWidth)
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
							qd := data_processing.NewQuorumDescription(nodePair[0], qs, h)
							qd.EndNodes = append(qd.EndNodes, nodePair[1])
							quorumDescriptions[nodePair[0]] = append(quorumDescriptions[nodePair[0]], qd)
						}
					} else {
						quorumDescriptions[nodePair[0]] = make([]*data_processing.QuorumDescription, 0)
						qd := data_processing.NewQuorumDescription(nodePair[0], qs, h)
						qd.EndNodes = append(qd.EndNodes, nodePair[1])
						quorumDescriptions[nodePair[0]] = append(quorumDescriptions[nodePair[0]], qd)
					}
				}
			}
		}
	}

	fmt.Println(pairsToHistograms)

	_, err = parseCSVFiles(*csvdir, pairsToHistograms, pairsToWindowAggregators, quorumDescriptions)
	if err != nil {
		fmt.Println("Error parsing CSV:", err)
		return
	}

	// Printing histogram data
	i := 0
	for _, bucket := range buckets {
		if !bucket.DoHistogram {
			continue
		}
		numHistograms := 0
		if bucket.QuorumSizes == nil {
			numHistograms = 1
		} else {
			numHistograms = len(bucket.QuorumSizes)
		}

		for j := 0; j < numHistograms; j++ {
			if histograms[i].Count() > 0 {
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

					if *crateImages {
						PlotHistogram(histograms[i], *outdir+"/histogram_"+lbl+".png", bucket.Label, true, 0)
						PlotHistogram(histograms[i], *outdir+"/histogram_"+lbl+"_tail.png", bucket.Label, true, 1000)
					}
				}
			}
			i += 1
		}
	}

	// Printing latency over time data
	i = 0
	for _, bucket := range buckets {
		if !bucket.DoWindowedLatencyAggregation {
			continue
		}
		numAggregators := 0
		if bucket.QuorumSizes == nil {
			numAggregators = 1
		} else {
			numAggregators = len(bucket.QuorumSizes)
		}

		for j := 0; j < numAggregators; j++ {
			lbl := strings.ReplaceAll(bucket.Label, " ", "_")
			fmt.Println("------------------------------------")
			fmt.Println("Experiment:", bucket.Label)
			fmt.Println("------------------------------------")

			if *outdir != "" {
				err = windowedAggregators[i].WriteToCSV(*outdir + "/latency" + lbl + ".csv")
				if err != nil {
					fmt.Println("Error writing CSV", err)
					return
				}

				if *crateImages {
					PlotAggregatedLatencyOverTime(windowedAggregators[i], *outdir+"/latency"+lbl+".png", bucket.Label)
				}
			}
			i += 1
			fmt.Println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
		}
	}
}

func parseCSVFiles(directoryPath string, pairsToHistograms map[string][]*data_processing.Histogram, pairsToWinAggregators map[string][]*data_processing.WindowAggregator, quorumDescriptions map[string][]*data_processing.QuorumDescription) ([]string, error) {
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

	epochTime := -1

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

		startRound := -1
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

			round, err := strconv.Atoi(record[1])

			if startRound == -1 {
				startRound = round
			}

			if startRound+*trimRawData > round {
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

			if epochTime == -1 {
				epochTime = startTime
			}

			latency := endTime - startTime

			for _, hist := range pairsToHistograms[nodePairStr] {
				hist.Add(latency)
			}

			for _, wa := range pairsToWinAggregators[nodePairStr] {
				wa.Add((startTime-epochTime)/1000, latency)
			}

			if qds, exists := quorumDescriptions[record[0]]; exists {
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
