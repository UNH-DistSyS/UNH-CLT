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
	"runtime"
	"strconv"
	"strings"
	"sync"
)

var csvdir = flag.String("csvdir", "", "location of csv files")
var outdir = flag.String("outdir", "", "location of output csv files")
var experimentsJson = flag.String("experiments_json", "", "location of JSON file with experimental description")
var histogramBucketWidth = flag.Int("hb", 5, "microseconds in each histogram bucket")
var windowWidth = flag.Int("ww", 1000, "milliseconds in each aggregated latency window")
var crateImages = flag.Bool("images", false, "whether to generate histogram images")
var trimRawData = flag.Int("trim", 0, "How many rounds to discard at the beginning of each data file")

func main() {
	flag.Parse()
	buckets, err := data_processing.ParseJSON(*experimentsJson)
	if err != nil {
		fmt.Println("Error parsing JSON:", err)
		return
	}

	numOutputs := 0
	for _, bucket := range buckets {
		if bucket.QuorumSizes == nil {
			numOutputs += 1
		} else {
			numOutputs += len(bucket.QuorumSizes)
		}
	}

	err = createDirIfNotExists(*outdir)
	if err != nil {
		fmt.Println("Error creating output directory", err)
		return
	}

	windowedAggregators := make(map[string]*data_processing.WindowAggregator, numOutputs)
	histograms := make(map[string]*data_processing.Histogram, numOutputs)

	pairsToHistograms := make(map[string][]*data_processing.Histogram)
	pairsToWindowAggregators := make(map[string][]*data_processing.WindowAggregator)

	qdLists := make(map[string][][]*data_processing.QuorumDescription)
	for _, bucket := range buckets {
		quorumDescriptions := make(map[string][]*data_processing.QuorumDescription)
		fmt.Println(bucket.Label)
		fmt.Println(bucket.NodePairs)

		if bucket.QuorumSizes == nil {
			fileNameStubs := bucket.GetFileNameStubs()
			if bucket.DoHistogram {
				h := data_processing.NewHistogram(1e4, *histogramBucketWidth)
				histograms[fileNameStubs[0]] = h
				for _, nodePair := range bucket.NodePairs {
					npStr := nodePair[0] + nodePair[1]
					if hist, exists := pairsToHistograms[npStr]; exists {
						hist = append(hist, histograms[fileNameStubs[0]])
					} else {
						pairsToHistograms[npStr] = make([]*data_processing.Histogram, 0)
						pairsToHistograms[npStr] = append(pairsToHistograms[npStr], h)
					}
				}
			}

			if bucket.DoWindowedLatencyAggregation {
				w := data_processing.NewWindowAggregator(*windowWidth)
				windowedAggregators[fileNameStubs[0]] = w
				for _, nodePair := range bucket.NodePairs {
					npStr := nodePair[0] + nodePair[1]
					if wa, exists := pairsToWindowAggregators[npStr]; exists {
						wa = append(wa, windowedAggregators[fileNameStubs[0]])
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
				_, fnStub := bucket.GetFileNameStubForQuorum(qs)
				histograms[fnStub] = h
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

			for nodeId, qds := range quorumDescriptions {
				if _, exists := qdLists[nodeId]; exists {
					qdLists[nodeId] = append(qdLists[nodeId], qds)
				} else {
					qdLists[nodeId] = make([][]*data_processing.QuorumDescription, 0)
					qdLists[nodeId] = append(qdLists[nodeId], qds)
				}
			}
		}
	}

	err, _, epochTime := parseCSVFilesParallel(*csvdir, pairsToHistograms, pairsToWindowAggregators, qdLists)
	if err != nil {
		fmt.Println("Error parsing CSV:", err)
		return
	}

	for _, wa := range windowedAggregators {
		wa.AdjustForEpochTime(epochTime)
	}

	// Printing histogram data
	for _, bucket := range buckets {
		if !bucket.DoHistogram {
			continue
		}
		fmt.Println("Looking at", bucket.Label)

		for _, fnStub := range bucket.GetFileNameStubs() {
			if histograms[fnStub] != nil {
				fmt.Println("histogram has", histograms[fnStub].Count(), "size")
			}
			if histograms[fnStub] != nil && histograms[fnStub].Count() > 0 {
				fmt.Println("Saving summary statistics and latency histogram data for", bucket.Label)

				if *outdir != "" {
					file, err := os.Create(*outdir + "/stats_" + fnStub + ".txt")
					if err != nil {
						fmt.Println("Error creating stats file:", err)
						return
					}

					printSummaryStatistics(file, bucket.Label, histograms[fnStub])

					file.Close()

					err = histograms[fnStub].WriteToCSV(*outdir + "/histogram_" + fnStub + ".csv")
					if err != nil {
						fmt.Println("Error writing CSV", err)
						return
					}

					if *crateImages {
						err = PlotHistogram(histograms[fnStub], *outdir+"/histogram_"+fnStub+".png", bucket.Label, true, 0)
						if err != nil {
							fmt.Println("Error plotting the latency histogram", err)
							return
						}
						err = PlotHistogram(histograms[fnStub], *outdir+"/histogram_"+fnStub+"_tail.png", bucket.Label, true, 1000)
						if err != nil {
							fmt.Println("Error plotting the latency histogram", err)
							return
						}
					}
				}
			}
		}
	}

	// Printing latency over time data
	for _, bucket := range buckets {
		if !bucket.DoWindowedLatencyAggregation {
			continue
		}

		for _, fnStub := range bucket.GetFileNameStubs() {
			fmt.Println("Saving windowed latency data for", bucket.Label)

			if *outdir != "" && windowedAggregators[fnStub] != nil {
				err = windowedAggregators[fnStub].WriteToCSV(*outdir + "/latency_" + fnStub + ".csv")
				if err != nil {
					fmt.Println("Error writing CSV", err)
					return
				}

				if *crateImages {
					err = PlotAggregatedLatencyOverTime(windowedAggregators[fnStub], *outdir+"/latency_"+fnStub+".png", bucket.Label)
					if err != nil {
						fmt.Println("Error plotting the latency overtime figure", err)
						return
					}
				}
			}
		}
	}
}

func printSummaryStatistics(file *os.File, label string, hist *data_processing.Histogram) {
	fmt.Fprintln(file, "------------------------------------")
	fmt.Fprintln(file, "Experiment:", label)
	fmt.Fprintln(file, "------------------------------------")
	fmt.Fprintf(file, "Number of Observations: %d\n", hist.Count())
	fmt.Fprintf(file, "Average Latency: %f ms\n", float64(hist.Mean())/1000)
	fmt.Fprintf(file, "Variance: %f ms\n", hist.Variance()/1000)
	fmt.Fprintf(file, "Std. Dev: %f ms\n", hist.StdDev()/1000)
	fmt.Fprintf(file, "Std. Err: %f ms\n", hist.StdErr()/1000)
	fmt.Fprintf(file, "25th Percentile Latency: %f ms\n", float64(hist.ApproxPercentile(0.25))/1000)
	fmt.Fprintf(file, "Median Latency: %f ms\n", float64(hist.ApproxPercentile(0.5))/1000)
	fmt.Fprintf(file, "75th Percentile Latency: %f ms\n", float64(hist.ApproxPercentile(0.75))/1000)
	fmt.Fprintf(file, "90th Percentile Latency: %f ms\n", float64(hist.ApproxPercentile(0.90))/1000)
	fmt.Fprintf(file, "95th Percentile Latency: %f ms\n", float64(hist.ApproxPercentile(0.95))/1000)
	fmt.Fprintf(file, "99th Percentile Latency: %f ms\n", float64(hist.ApproxPercentile(0.99))/1000)
	fmt.Fprintf(file, "99.9th Percentile Latency: %f ms\n", float64(hist.ApproxPercentile(0.999))/1000)
	fmt.Fprintf(file, "99.99th Percentile Latency: %f ms\n", float64(hist.ApproxPercentile(0.9999))/1000)
	fmt.Fprintf(file, "99.999th Percentile Latency: %f ms\n", float64(hist.ApproxPercentile(0.99999))/1000)
	fmt.Fprintf(file, "Max Latency: %f ms\n", float64(hist.Max())/1000)
	fmt.Fprintf(file, "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n")
}

func parseCSVFilesParallel(directoryPath string, pairsToHistograms map[string][]*data_processing.Histogram, pairsToWinAggregators map[string][]*data_processing.WindowAggregator, quorumDescriptions map[string][][]*data_processing.QuorumDescription) (error, []string, int) {
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
		return err, nil, -1
	}

	var etlock sync.Mutex
	epochTimeMs := int((^uint(0)) >> 1)

	// Parse all CSV files
	wg := sync.WaitGroup{}
	wg.Add(len(filenames))
	semaphore := make(chan struct{}, runtime.NumCPU()*2)
	numFiles := len(filenames)
	processedFiles := 0
	for _, fn := range filenames {
		go func(filename string) {
			localEpochTimeMicroSeconds := -1
			semaphore <- struct{}{}
			//fmt.Println("Working on", filename)
			var file *os.File
			var reader *csv.Reader

			if strings.HasSuffix(filename, ".gz") {
				// If the file is gzipped, decompress it first
				file, err = os.Open(filename)
				if err != nil {
					fmt.Println("Error:", err)
					return
				}
				defer file.Close()

				gzipReader, err := gzip.NewReader(file)
				if err != nil {
					fmt.Println("Error:", err)
					return
				}
				defer gzipReader.Close()

				reader = csv.NewReader(gzipReader)
			} else {
				// If the file is not gzipped, just open it normally
				file, err = os.Open(filename)
				if err != nil {
					fmt.Println("Error:", err)
					return
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
					fmt.Println("Error:", err)
					return
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

				if localEpochTimeMicroSeconds == -1 || localEpochTimeMicroSeconds > startTime {
					localEpochTimeMicroSeconds = startTime
				}

				latency := endTime - startTime

				for _, hist := range pairsToHistograms[nodePairStr] {
					hist.Add(latency)
				}

				if latency > 800000 {
					fmt.Printf("%s: %s -> %s: %d microseconds\n", record[1], record[0], record[2], latency)
				}

				for _, wa := range pairsToWinAggregators[nodePairStr] {
					wa.Add((startTime)/1000, latency)
				}

				if qdList, exists := quorumDescriptions[record[0]]; exists {
					if err != nil {
						continue
					}

					for _, qds := range qdList {
						for _, qd := range qds {
							qd.AddQuorumLatency(latency, round, record[2])
						}
					}
				}

			}

			etlock.Lock()
			if localEpochTimeMicroSeconds/1000 < epochTimeMs {
				epochTimeMs = localEpochTimeMicroSeconds / 1000
			}
			etlock.Unlock()

			processedFiles += 1

			fmt.Printf("\rProcessed: %d/%d: %s", processedFiles, numFiles, getProgressBarStr(50, float64(processedFiles)/float64(numFiles)))
			<-semaphore
			wg.Done()
		}(fn)
	}

	wg.Wait()
	fmt.Println("\nDone processing CSVs. Computing statistics")
	return nil, filenames, epochTimeMs
}

func createDirIfNotExists(path string) error {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		err = os.Mkdir(path, 0755)
	}
	return err
}

func getProgressBarStr(strLen int, progress float64) string {
	progressSpace := int(float64(strLen) * progress)
	progressStr := "["
	for i := 0; i < strLen; i++ {
		if progressSpace > 0 {
			progressStr += "="
			progressSpace -= 1
		} else {
			progressStr += " "
		}
	}
	progressStr += "]"
	return progressStr
}
