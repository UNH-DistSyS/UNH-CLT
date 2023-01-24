package config

import (
	"encoding/json"
	"os"

	"github.com/UNH-DistSyS/UNH-CLT/log"
)

// BConfig Represents the configuration of the entire node.

type BConfig struct {
	ClientType           string  `json:"client_type"`           // type of client connector to use: kv-http, cql-http, cql-tcp
	T                    int     `json:"t"`                     // total number of running time in seconds
	N                    int     `json:"n"`                     // total number of requests
	Min                  int     `json:"min"`                   // min key
	K                    int     `json:"k"`                     // key space size. So a keyspace is [0, K].
	W                    float64 `json:"w"`                     // write ratio
	Throttle             int     `json:"throttle"`              // requests per second throttle, unused if 0
	Concurrency          int     `json:"concurrency"`           // number of simulated clients
	Distribution         string  `json:"distribution"`          // distribution
	LinearizabilityCheck bool    `json:"linearizability_check"` // run linearizability checker at the end of benchmark
	PreferredIds         []int   `json:"preferred_ids"`         // node ids in the bench zone to prefer. If missing, using random
	Size                 int     `json:"size"`                  // Payload size
	ObjSharing           int     `json:"obj_sharing"`           // object sharing mode -- number indicates how many regions share an object. 0 - disabled
	PrefferLeader        bool    `json:"prefer_leader"`         // whether the benchmark will prefer to use the leader node reported in previous operation. This makes sense to use for stable-leader, non-partitioned use cases

	// conflict distribution
	Conflicts int `json:"conflicts"` // percentage of conflicting keys

	// normal distribution
	Mu    float64 `json:"mu"`    // mu of normal distribution
	Sigma float64 `json:"sigma"` // sigma of normal distribution
	Move  bool    `json:"move"`  // moving average (mu) of normal distribution
	Speed int     `json:"speed"` // moving speed in milliseconds intervals per key

	// zipfian distribution
	ZipfianTheta float64 `json:"zipfian_theta"` // zipfian skew measure theta

	// exponential distribution
	Lambda float64 `json:"lambda"` // rate parameter

	// Native cassandra address
	NativeCassandraAddresses []string `json:"native_cassandra_addresses"` // for native native cassandra support
}

func MakeDefaultBConfig() *BConfig {
	return &BConfig{
		ClientType:               "cql-tcp",
		T:                        60,
		N:                        0,
		K:                        500,
		W:                        0.5,
		Throttle:                 0,
		Concurrency:              1,
		Size:                     128,
		PrefferLeader:            false,
		LinearizabilityCheck:     true,
		PreferredIds:             []int{},
		Distribution:             "uniform",
		Conflicts:                0,
		Min:                      0,
		Mu:                       0,
		Sigma:                    60,
		Move:                     false,
		Speed:                    500,
		ZipfianTheta:             0.99,
		Lambda:                   0.01,
		NativeCassandraAddresses: []string{"127.0.0.1"},
		ObjSharing:               0,
	}
}

func LoadBConfigFromFile(configFile string) *BConfig {
	cfg := new(BConfig)
	err := cfg.load(configFile)
	if err != nil {
		log.Fatal(err)
		return nil
	}
	// id can be specified in flag if left blank in config
	return cfg
}

// String is implemented to print the config
func (c *BConfig) String() string {
	config, err := json.Marshal(c)
	if err != nil {
		log.Errorln(err)
	}
	return string(config)
}

// load configurations from config file in JSON format
func (c *BConfig) load(configFile string) error {
	file, err := os.Open(configFile)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(file)
	err = decoder.Decode(c)
	if err != nil {
		return err
	}
	return nil
}

// Save save configurations to file in JSON format
func (c *BConfig) Save(configFile string) error {
	file, err := os.Create(configFile)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(file)
	return encoder.Encode(c)
}
