package config

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/UNH-DistSyS/UNH-CLT/ids"
)

const (
	BASIC_NODE = "basic"
)

type ClusterMembershipConfig struct {
	NumNode     int `json:"Num_Node"`
	NumWriter   int `json:"Num_Writer"`
	ThinkTimeUS int `json:"thinktime_us"`
	RunTimeS    int `json:"run_time_s"`

	AddrsStr     map[string]string `json:"address"`      // address for node communication
	HTTPAddrsStr map[string]string `json:"http_address"` // address for bench node_assembler communication

	Regions     map[uint8]string  `json:"regions"`     // map of regionIDs to a string label
	RegionZones map[uint8][]uint8 `json:"regionZones"` // map of regionID to a list of zones in the region

	/************************************************************************************************************
	 * Here are config parameters derived from the json file
	 ***********************************************************************************************************/

	IDs            []ids.ID           // list of all IDs.
	ZonesToNodeIds map[uint8][]ids.ID // map of zones to ids in these zones
	Addrs          map[ids.ID]string  // address for node communication
	HTTPAddrs      map[ids.ID]string  // address for http client communication

	sync.RWMutex // mutex to protext configuration from concurrent access
}

func MakeDefaultClusterMembershipConfig() *ClusterMembershipConfig {
	config := new(ClusterMembershipConfig)
	config.Addrs = map[ids.ID]string{*ids.GetIDFromString("1.1"): "tcp://127.0.0.1:" + strconv.Itoa(PORT)}
	config.HTTPAddrs = map[ids.ID]string{*ids.GetIDFromString("1.1"): "http://localhost:" + strconv.Itoa(HTTP_PORT)}
	config.Regions = map[uint8]string{1: "region_1"}
	config.RegionZones = map[uint8][]uint8{1: {1}}
	config.RefreshIdsFromAddresses()
	return config
}

func (c *ClusterMembershipConfig) GetIds() []ids.ID {
	c.RLock()
	defer c.RUnlock()
	return c.IDs
}

func (c *ClusterMembershipConfig) GetNodeIdsInZone(zone uint8) []ids.ID {
	c.RLock()
	defer c.RUnlock()
	return c.ZonesToNodeIds[zone]
}

func (c *ClusterMembershipConfig) GetZoneIds() []uint8 {
	c.RWMutex.Lock()
	defer c.RWMutex.Unlock()
	zones := make([]uint8, 0)
	for id := range c.Addrs {
		idInZones := false
		for _, z := range zones {
			if z == id.Zone() {
				idInZones = true
				break
			}
		}
		if !idInZones {
			zones = append(zones, id.Zone())
		}
	}
	return zones
}

func (c *ClusterMembershipConfig) RefreshIdsFromAddresses() {
	c.IDs = make([]ids.ID, 0, len(c.Addrs))
	c.ZonesToNodeIds = make(map[uint8][]ids.ID)
	for id := range c.Addrs {
		c.IDs = append(c.IDs, id)
		if c.ZonesToNodeIds[id.ZoneId] == nil {
			c.ZonesToNodeIds[id.ZoneId] = make([]ids.ID, 0)
		}
		c.ZonesToNodeIds[id.ZoneId] = append(c.ZonesToNodeIds[id.ZoneId], id)
	}

	if c.Regions == nil {
		c.Regions = map[uint8]string{1: "region_1"}
		c.RegionZones = map[uint8][]uint8{1: c.GetZoneIds()}
	}
}

func (c *ClusterMembershipConfig) AddZone(zoneId, regionId uint8) {
	c.RWMutex.Lock()
	defer c.RWMutex.Unlock()
	c.RegionZones[regionId] = append(c.RegionZones[regionId], zoneId)
}

func (c *ClusterMembershipConfig) AddNodeAddress(id ids.ID, serverAddr, httpAddress string) error {
	c.RWMutex.Lock()
	defer c.RWMutex.Unlock()
	for _, existingId := range c.IDs {
		if existingId == id {
			return fmt.Errorf("node with id=%v already exists and cannot be added. Are you tryitn to replace an existing node?", id)
		}
	}

	c.Addrs[id] = serverAddr
	c.HTTPAddrs[id] = httpAddress
	c.IDs = append(c.IDs, id)
	if c.ZonesToNodeIds[id.ZoneId] == nil {
		c.ZonesToNodeIds[id.ZoneId] = make([]ids.ID, 0)
	}
	c.ZonesToNodeIds[id.ZoneId] = append(c.ZonesToNodeIds[id.ZoneId], id)
	return nil
}

func (c *ClusterMembershipConfig) Init() {
	c.Lock()
	defer c.Unlock()
	// get a list of all addresses in a map of ids
	c.Addrs = make(map[ids.ID]string, len(c.AddrsStr))
	for idStr, addr := range c.AddrsStr {
		id := ids.GetIDFromString(idStr)
		c.Addrs[*id] = addr
	}

	// get a list of all addresses in a map of ids
	c.HTTPAddrs = make(map[ids.ID]string, len(c.HTTPAddrsStr))
	for idStr, addr := range c.HTTPAddrsStr {
		id := ids.GetIDFromString(idStr)
		c.HTTPAddrs[*id] = addr
	}
	c.RefreshIdsFromAddresses()
}
