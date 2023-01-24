package kv_table

import (
	"github.com/UNH-DistSyS/UNH-CLT/replication/operations"
	"github.com/UNH-DistSyS/UNH-CLT/replication/operations/readop"
	"github.com/UNH-DistSyS/UNH-CLT/replication/operations/writeop"
	"github.com/UNH-DistSyS/UNH-CLT/store"
	"github.com/google/uuid"
)

type KVTable interface {
	Close()
	Size() int
	Read(readOp *readop.ReadOp) ([]store.KVItem, error)
	Write(payload *writeop.WriteOp) error
	SlowDeleteAllFound(deleteSpec *operations.DeleteOp) error
	ReadConflictDomainVersion(cdkey store.ByteString) (uint64, error)
	String() string

	GetTableName() string
	GetUUID() uuid.UUID
}
