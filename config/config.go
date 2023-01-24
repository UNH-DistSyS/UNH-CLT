package config

import (
	"encoding/json"
	"os"

	"github.com/google/uuid"

	"github.com/UNH-DistSyS/UNH-CLT/log"
)

// default values
const (
	PORT             = 1735
	HTTP_PORT        = 8080
	CHAN_BUFFER_SIZE = 1024 * 1

	MAX_GAP       = 1024
	BUFFER_SIZE   = int(MAX_GAP * 8)
	REMAIN_SIZE   = MAX_GAP * 16
	PRUNE_TRIGGER = MAX_GAP * 32

	SCHEDULED_TICKER_MS         = 10
	OP_DISPATCHER_CONCURRENCY   = 5
	REPAIR_MULTIPLE             = 5
	DEFAULT_REPLACTION_PROTOCOL = "mpaxos"
	DEFAULT_WAL_TYPE            = "memory"
)

/*******************************************************************************
* Hardcoded definitions of some system-level KV-tables
* 1: Tables table - lists IDs and names of tables
* 2: ConflictDomains table - list of all ConflictDomainKeys stored on the node.
********************************************************************************/
var LocusDBTablesUUID, _ = uuid.Parse("241aa2a0-b730-11ea-aca5-bd0d82bb57cd")
var LocusDBTablesTableName = "locus_tables"

var LocusDBConflictDomainsTableUUID, _ = uuid.Parse("7973fbcc-e2d5-4334-bdc5-06e5a7bcabce")
var LocusDBConflictDomainsTableName = "locus_cds"

/**
 * Represents the configuration of the entire node.
 */

type Config struct {
	ClusterMembership ClusterMembershipConfig `json:"cluster_membership"`

	Store StoreConfig `json:"store"`

	StickyLeader    bool `json:"sticky_leader"`     // Sticky leader, if true paxos forward request to current leader
	LeaderHintSlack int  `json:"leader_hint_slack"` // how many ms must elapse before the current leader can suggest a new leader change hint
	ChanBufferSize  int  `json:"chan_buffer_size"`  // size of all internal channels used for passing work between layers/modules
	NetBufferSize   int  `json:"net_buffer_size"`   // size of network transfer buffers

	WalBufferSize         int `json:"buffer_size"`             // default buffer size for memory WAL implementation
	OpDispatchConcurrency int `json:"op_dispatch_concurrency"` // concurrency level for concurrent dispatcher

	ScheduledTickerMS uint64 `json:"Scheduled_Ticker_MS"` // ticker for scheduled functions

	PruneTriggerSize     uint64 `json:"Prune_Trigger"`      // log will start pruning after reaching this size
	RemainSizeAfterPrune uint64 `json:"Remain_After_Prune"` // log size remaining after pruning

	MaxIdleHttpCnx int `json:"max_idle_connections"` // http max http idle connections per host

	Quorum              string `json:"quorum"`               // type of the quorums. Some protocol may support different quorums
	ReplicationProtocol string `json:"replication_protocol"` // what protocol to use for replication
	F                   int    `json:"f"`                    // node failures per zone or per protocol instance depending on the replication protocol
	MaxAllowedLogGap    uint64 `json:"max_allowed_log_gap"`  // max number of items in the log after a gap before forcing gap slot recovery

	MaxZoneFailures  int               `json:"max_zone_failures"`  // number of failure zones in general grid quorums
	StaticReplGroups map[uint8][]uint8 `json:"replication_groups"` // map of zoneId to other zones in the same replication group
	DynamicReplGroup bool              `json:"dynamic_repl_group"` // whether to use dynamic replication group - compute it from the closest zones.
	ReplGroupSize    int               `json:"repl_group_size"`    // How many zones are in the dynamic replication group.
	FullReplication  bool              `json:"full_replication"`   // whether to replicate outside of the Replication Group.

	// Server-side simeouts
	CommunicationTimeoutMs    int `json:"communication_timeout_ms"` // a timeout for a single attempt of round-trip communication
	RoundTimeoutMs            int `json:"round_timeout_ms"`         // round timeout. A round may have multiple communication attempts
	RequestTimeoutMs          int `json:"request_timeout_ms"`       // request timeout on server side
	PaxosRepairRoundTimeoutMs int // Maximum allowed time for a repair. A constant multiple of a round timeout
	PaxosRepairTimeoutMs      int // Maximum allowed time for a repair attempt. A constant multiple of a repair round timeout

	WALType string `json:"wal_type"`

	ProximityUncertainty float64 `json:"proximity_uncertainty"` // the variation in proximity readings that causes no proximity updates

	// Policy Configurations
	MigrationPolicy string `json:"placement_oracle"` // migration policy to use: cog, ema, n-consecutive

	NConsecutive uint8 `json:"n_consecutive"` // number of consecutive accesses to leader change

	EmaAlpha   float64 `json:"ema_alpha"`
	EmaEpsilon float64 `json:"ema_epsilon"`

	WindowType       int   `json:"window_type"`         // type of COG window. Currently, either time-based or number of request based tumbling windows
	WindowSize       uint8 `json:"window_size"`         // window size
	TimeWindowSizeMs int32 `json:"time_window_size_ms"` // size of time window in ms
	MinWindowReq     uint8 `json:"min_window_req"`      // smallest number of requests in a time-based window

	CogScoreUncertainty float64 `json:"cog_score_uncertainty"` // the uncertainty between two scores that will be ignored. i.e., 0.05 for 5%

	// Health Checker Configuration
	HealthMonitor HealthMonitorConfig `json:"health_monitor"`

	// Balancer Configuration
	OverloadPreventingBalancer OverloadPreventingBalancerConfig `json:"overload_preventing_balancer"`
	LoadBalancer               string                           `json:"load_balancer"`           // type of load balancer used for migration: random, ownership, request
	BalGossipInt               int64                            `json:"balance_gossip_interval"` // Interval between balance data gossip messages in ms
	OverldThrshld              float64                          `json:"overload_threshold"`      // weight overload threshold for balancers that use float weight scores

	Client ClientConfig `json:"client"`
}

func MakeDefaultConfig() *Config {
	config := new(Config)
	config.ClusterMembership = *MakeDefaultClusterMembershipConfig()
	config.Client = *NewDefaultClientConfig()
	config.ReplicationProtocol = DEFAULT_REPLACTION_PROTOCOL
	config.Quorum = "fgrid"
	config.Store = *NewDefaultStoreConfig()
	config.StickyLeader = true
	config.ChanBufferSize = CHAN_BUFFER_SIZE
	config.NetBufferSize = CHAN_BUFFER_SIZE * 10
	config.ScheduledTickerMS = SCHEDULED_TICKER_MS

	//by default, sizes should depending on the max_allowed_log_gap for safety reasons.
	config.MaxAllowedLogGap = MAX_GAP
	config.WalBufferSize = BUFFER_SIZE
	config.PruneTriggerSize = PRUNE_TRIGGER
	config.RemainSizeAfterPrune = REMAIN_SIZE

	config.OpDispatchConcurrency = OP_DISPATCHER_CONCURRENCY
	config.BalGossipInt = 1000
	config.LeaderHintSlack = 1000
	config.OverldThrshld = 0.02
	config.MaxIdleHttpCnx = 100
	config.ProximityUncertainty = 0.05
	config.WALType = DEFAULT_WAL_TYPE
	config.StaticReplGroups = map[uint8][]uint8{uint8(1): {uint8(1)}, uint8(2): {uint8(2)}, uint8(3): {uint8(3)}}
	config.DynamicReplGroup = false
	config.FullReplication = true
	config.MigrationPolicy = "cog"
	config.WindowSize = 10
	config.TimeWindowSizeMs = 100
	config.WindowType = 1
	config.MinWindowReq = 5
	config.CogScoreUncertainty = 0.05
	config.NConsecutive = 3
	config.EmaAlpha = 0.5
	config.EmaEpsilon = 0.1
	config.RequestTimeoutMs = 900
	config.RoundTimeoutMs = 400
	config.PaxosRepairRoundTimeoutMs = config.RoundTimeoutMs * REPAIR_MULTIPLE
	config.PaxosRepairTimeoutMs = config.PaxosRepairRoundTimeoutMs * REPAIR_MULTIPLE
	config.CommunicationTimeoutMs = 100
	config.LoadBalancer = "random"
	config.OverloadPreventingBalancer = NewDefaultOverloadPreventingBalancerConfig()
	config.HealthMonitor = NewDefaultHealthMonitorConfig()
	config.ClusterMembership.RefreshIdsFromAddresses()
	config.F = 0
	config.MaxZoneFailures = 0
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

	// do the derived configs
	c.PaxosRepairTimeoutMs = c.RoundTimeoutMs * REPAIR_MULTIPLE
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
