package config

import (
	"fmt"
	"sync"

	"github.com/UNH-DistSyS/UNH-CLT/ids"
)

type NodeInfo struct {
	NodeId         ids.ID
	PrivateAddress string
	PublicAddress  string
	CSV_prefix     string
	RSA_path       string
	Usr_name       string
}

type ClusterMembershipConfig struct {
	// we use two types of addresses: private and public. Private addresses are used for talking within the regional
	// network, while public addresses are for talking across regions
	PrivateAddrsStr map[string]string `json:"private_address"` // private address for node communication
	PublicAddrsStr  map[string]string `json:"public_address"`  // public address for node communication if different from private.
	CSV_prefix      map[string]string `json:"csv_prefix"`      //CSV prefix
	RSA_path        map[string]string `json:"rsa_path"`        // RSA key location
	Usr_name        map[string]string `json:"usr_name"`        // usr name for ssh

	/************************************************************************************************************
	 * Here are config parameters derived from the json file
	 ***********************************************************************************************************/

	IDs            []ids.ID           // list of all IDs.
	ZonesToNodeIds map[uint8][]ids.ID // map of zones to ids in these zones
	//PrivateAddrs   map[ids.ID]string  // private address for node communication
	//PublicAddrs    map[ids.ID]string  // public address for node communication

	Addrs map[ids.ID]NodeInfo // node info for communication

	sync.RWMutex // mutex to protext configuration from concurrent access
}

func MakeDefaultClusterMembershipConfig() *ClusterMembershipConfig {
	config := new(ClusterMembershipConfig)
	config.Addrs = make(map[ids.ID]NodeInfo)
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

func (c *ClusterMembershipConfig) AddNodeAddress(id ids.ID, privateServerAddr, publicServerAddr string) error {
	c.RWMutex.Lock()
	defer c.RWMutex.Unlock()
	for _, existingId := range c.IDs {
		if existingId == id {
			return fmt.Errorf("node with id=%v already exists and cannot be added. Are you tryitn to replace an existing node?", id)
		}
	}

	ni := NodeInfo{
		NodeId:         id,
		PrivateAddress: privateServerAddr,
		PublicAddress:  publicServerAddr,
	}

	c.Addrs[id] = ni
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
	c.Addrs = make(map[ids.ID]NodeInfo, len(c.PrivateAddrsStr))
	for idStr, addr := range c.PrivateAddrsStr {
		id := ids.GetIDFromString(idStr)

		ni := NodeInfo{
			NodeId:         *id,
			PrivateAddress: addr,
			PublicAddress:  c.PublicAddrsStr[idStr],
			CSV_prefix:     c.CSV_prefix[idStr],
			RSA_path:       c.RSA_path[idStr],
			Usr_name:       c.Usr_name[idStr],
		}

		c.Addrs[*id] = ni

	}

	c.RefreshIdsFromAddresses()
}
