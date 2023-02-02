package config

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/UNH-DistSyS/UNH-CLT/ids"
)

type ClusterMembershipConfig struct {
	AddrsStr map[string]string `json:"address"` // address for node communication

	/************************************************************************************************************
	 * Here are config parameters derived from the json file
	 ***********************************************************************************************************/

	IDs            []ids.ID           // list of all IDs.
	ZonesToNodeIds map[uint8][]ids.ID // map of zones to ids in these zones
	Addrs          map[ids.ID]string  // address for node communication

	sync.RWMutex // mutex to protext configuration from concurrent access
}

func MakeDefaultClusterMembershipConfig() *ClusterMembershipConfig {
	config := new(ClusterMembershipConfig)
	config.Addrs = map[ids.ID]string{*ids.GetIDFromString("1.1"): "tcp://127.0.0.1:" + strconv.Itoa(PORT)}
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

	c.RefreshIdsFromAddresses()
}
