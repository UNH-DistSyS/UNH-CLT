package config

import (
	"encoding/json"
	"os"

	"github.com/UNH-DistSyS/UNH-CLT/log"
)

// default values
const (
	PORT                      = 1735
	CHAN_BUFFER_SIZE          = 1024 * 1
	DEFAULT_RATE              = 10
	OP_DISPATCHER_CONCURRENCY = 5
	DEFAULT_PAYLOAD           = 1024
	SELF_LOOP                 = true
)

/**
 * Represents the configuration of the entire node.
 */

type Config struct {
	ClusterMembership     ClusterMembershipConfig `json:"cluster_membership"`
	ChanBufferSize        int                     `json:"chan_buffer_size"`        // size of all internal channels used for passing work between layers/modules
	NetBufferSize         int                     `json:"net_buffer_size"`         // size of network transfer buffers
	OpDispatchConcurrency int                     `json:"op_dispatch_concurrency"` // concurrency level for concurrent dispatcher
	PayLoadSize           int                     `json:"payload_size"`            // size of payload
	SelfLoop              bool                    `json:"self_loop"`               // whether to send ping messages to self

	TestingRate uint64 `json:"testing_rate"` // rate at which a node produces ping-pong rounds.

	// Server-side simeouts
	CommunicationTimeoutMs int `json:"communication_timeout_ms"` // a timeout for a single attempt of round-trip communication
	RoundTimeoutMs         int `json:"round_timeout_ms"`         // round timeout. A round may have multiple communication attempts
	RequestTimeoutMs       int `json:"request_timeout_ms"`       // request timeout on server side
}

func MakeDefaultConfig() *Config {
	config := new(Config)
	config.ClusterMembership = *MakeDefaultClusterMembershipConfig()
	config.ChanBufferSize = CHAN_BUFFER_SIZE
	config.NetBufferSize = CHAN_BUFFER_SIZE * 10
	config.TestingRate = DEFAULT_RATE
	config.OpDispatchConcurrency = OP_DISPATCHER_CONCURRENCY
	config.SelfLoop = SELF_LOOP
	config.PayLoadSize = DEFAULT_PAYLOAD

	config.RequestTimeoutMs = 900
	config.RoundTimeoutMs = 400
	config.CommunicationTimeoutMs = 100
	config.ClusterMembership.RefreshIdsFromAddresses()
	return config
}

func LoadConfigFromFile(configFile string) *Config {
	// start from default, make sure nothing is missed
	cfg := MakeDefaultConfig()
	err := cfg.load(configFile)
	if err != nil {
		log.Fatal(err)
		return nil
	}
	// id can be specified in flag if left blank in config
	return cfg
}

// String is implemented to print the config
func (c *Config) String() string {
	config, err := json.Marshal(c)
	if err != nil {
		log.Errorln(err)
	}
	return string(config)
}

// load configurations from config file in JSON format
func (c *Config) load(configFile string) error {
	file, err := os.Open(configFile)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(file)
	err = decoder.Decode(c)
	if err != nil {
		return err
	}

	c.ClusterMembership.Init()

	return nil
}

// Save save configurations to file in JSON format
func (c *Config) Save(configFile string) error {
	file, err := os.Create(configFile)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(file)
	return encoder.Encode(c)
}
