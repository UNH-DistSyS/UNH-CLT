package data_processing

import (
	"encoding/json"
	"os"
	"sort"
)

type Bucket struct {
	Label                        string     `json:"label"`
	NodePairs                    [][]string `json:"node_pairs"`
	QuorumSizes                  []int      `json:"quorum_sizes"`
	DoHistogram                  bool       `json:"histogram"`
	DoWindowedLatencyAggregation bool       `json:"windowed_latency_aggregation"`
}

type QuorumDescription struct {
	StartNode string
	EndNodes  []string
	Size      int

	roundLatencies map[int][]int // round -> list of latencies
	Histogram      *Histogram
}

func NewQuorumDescription(startNode string, size int, h *Histogram) *QuorumDescription {
	return &QuorumDescription{
		StartNode:      startNode,
		EndNodes:       make([]string, 0),
		Size:           size,
		roundLatencies: make(map[int][]int, 0),
		Histogram:      h,
	}
}

func (qd *QuorumDescription) IsValidEndNode(id string) bool {
	for _, endNodeId := range qd.EndNodes {
		if endNodeId == id {
			return true
		}
	}
	return false
}

func (qd *QuorumDescription) AddQuorumLatency(latency, round int, nodeId string) {
	if qd.IsValidEndNode(nodeId) {
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

func ParseJSON(filename string) ([]Bucket, error) {
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
