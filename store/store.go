package store

import (
	"github.com/google/uuid"
	"github.com/syndtr/goleveldb/leveldb/errors"
)

var (
	ErrStoreError             = errors.New("LevelDB error")
	ErrStoreGetError          = errors.New("LevelDB error during Get operation")
	ErrNotFound               = errors.New("LevelDB: item not found")
	ErrStorePutError          = errors.New("LevelDB error during Put operation")
	ErrStoreDelError          = errors.New("LevelDB error during Delete operation")
	ErrTableNotFound          = errors.New("Table Not Found")
	ErrIncorrectReplicationOp = errors.New("Incorrect replication operation")
)

type StoreType uint8

const (
	KV StoreType = iota
	CQL
)

type BaseTable struct {
	tableName 		string
	uuid 			uuid.UUID
}

func NewBaseTable(name string, uuid uuid.UUID) BaseTable {
	bt := new(BaseTable)
	bt.tableName = name
	bt.uuid = uuid
	return *bt
}

func (bt *BaseTable) GetTableName() string {
	return bt.tableName
}

func (bt *BaseTable) GetUUID() uuid.UUID {
	return bt.uuid
}