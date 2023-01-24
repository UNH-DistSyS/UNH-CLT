package ids

import (
	"flag"
	"strconv"
	"strings"

	"github.com/UNH-DistSyS/UNH-CLT/log"
)

var id = flag.String("id", "1.1", "ID in format of Zone.Node. Default 1.1. For client ids add .c at the end: 1.1.c")

// ID represents a generic identifier in format of Zone.Node
//type ID string

type ID struct {
	NodeId   uint8
	ZoneId   uint8
	IsClient bool // designates this ID as one belonging to a client_driver. Clients need id to keep track of zones and locality
	//StrId    string
}

func NewID(zone, node uint8) *ID {
	return &ID{NodeId: node, ZoneId: zone, IsClient: false}
}

func NewClientID(zone, node uint8) *ID {
	return &ID{NodeId: node, ZoneId: zone, IsClient: true}
}

// GetIDFromFlag gets the current id specified in flag variables
func GetIDFromFlag() *ID {
	if !flag.Parsed() {
		log.Warningln("Using GetIDFromFlag before parsing flags")
	}
	return GetIDFromString(*id)
}

func GetIDFromString(id string) *ID {
	if !strings.Contains(id, ".") {
		log.Warningf("id %s does not contain \".\"\n", id)
		return nil
	}
	idParts := strings.Split(id, ".")
	if len(idParts) > 1 {
		zone, err := strconv.ParseUint(idParts[0], 10, 64)
		if err != nil {
			log.Errorf("Failed to convert Zone %s to int\n", idParts[0])
		}

		node, err := strconv.ParseUint(idParts[1], 10, 64)
		if err != nil {
			log.Errorf("Failed to convert Node %s to int\n", idParts[1])
		}
		isClient := false
		if len(idParts) == 3 && idParts[2] == "c" {
			isClient = true
		}
		return &ID{ZoneId: uint8(zone), NodeId: uint8(node), IsClient: isClient}
	} else {
		log.Errorf("Could not parse ID %s", id)
		return nil
	}
}

// Zone returns Zone ID component
func (i *ID) Zone() uint8 {
	return i.ZoneId
}

// Node returns Node ID component
func (i *ID) Node() uint8 {
	return i.NodeId
}

func (i *ID) IsClientId() bool {
	return i.IsClient
}

func (i ID) String() string {
	s := strconv.Itoa(int(i.ZoneId)) + "." + strconv.Itoa(int(i.NodeId))
	if i.IsClient {
		s += ".c"
	}
	return s
}

func (i ID) Int() int {
	return int(i.ZoneId)<<8 | int(i.NodeId)
}
