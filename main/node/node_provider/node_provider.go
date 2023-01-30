package node_provider

import (
	"log"
	"os"

	"github.com/UNH-DistSyS/UNH-CLT/config"
)

func CreateNewNode(cfg *config.Config) database.WAL {
	var wal database.WAL
	switch cfg.NodeType {
	case "basic":
		wal = memorylog.NewMemLog(cfg.WalBufferSize)
	case "ringbuffer":
		wal = ringbuffer.NewRingBuffer(cfg.WalBufferSize)
	default:
		log.Fatalf("Cannot create WAL: configuration does not specify correct WAL implementation")
		os.Exit(-1)
	}

	return wal
}
