package leveldb_kv_table

import (
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/log"
	"github.com/UNH-DistSyS/UNH-CLT/replication/operations"
	"github.com/UNH-DistSyS/UNH-CLT/replication/operations/readop"
	"github.com/UNH-DistSyS/UNH-CLT/replication/operations/writeop"
	"github.com/UNH-DistSyS/UNH-CLT/store"
	"github.com/UNH-DistSyS/UNH-CLT/utils"
	"github.com/google/uuid"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// leveldb database maintains the key-value datastore

type LevelDBKVTable struct {
	ids.ID // must have an identity
	store.BaseTable
	*sync.RWMutex
	leveldb *leveldb.DB
}

// NewTable get the instance of table LevelDB Wrapper
func NewLevelDBTable(identity ids.ID,
	tableLocation string,
	name string,
	id uuid.UUID) *LevelDBKVTable {
	log.Infof("Creating LevelDB KV-table %v at node %v \n", name, identity)
	tbl := new(LevelDBKVTable)
	tbl.ID = identity
	tbl.BaseTable = store.NewBaseTable(name, id)
	tbl.RWMutex = new(sync.RWMutex)
	lvlDBName := tableLocation + "/" + identity.String() + "/" + id.String() + "/" + name
	lvldb, err := leveldb.OpenFile(lvlDBName, nil)
	if err != nil {
		log.Errorf("Could not create levelDB storage for kv_table %s: %v", name, err)
		return nil
	}
	tbl.leveldb = lvldb
	return tbl
}

func (tbl *LevelDBKVTable) Close() {
	tbl.leveldb.Close()
}

func (tbl *LevelDBKVTable) Size() int {
	return 0
}

// We generally do not wrap individual reads and writes into a transactions, since we control the concurrency
// at a higher level - Paxos and executor. So only one operation can access a partition at a time
func (tbl *LevelDBKVTable) Read(readOp *readop.ReadOp) ([]store.KVItem, error) {
	log.Debugf("Read against LevelDB on node %v. ReadMode=%v", tbl.ID, readOp.ReadMode)
	s := time.Now()
	switch readOp.ReadMode {
	case readop.POINT:
		v, err := tbl.leveldb.Get(readOp.StartKey, nil)
		if err != nil {
			if err.Error() == "leveldb: not found" {
				return nil, store.ErrNotFound
			}
			log.Errorln(err)
			return nil, store.ErrStoreGetError
		}
		kvItems := []store.KVItem{{
			Key:   readOp.StartKey,
			Value: v,
		}}
		e := time.Now()
		if e.Sub(s).Milliseconds() > 50 {
			log.Warningf("Node %v experienced a long point read on table %s. Point read took %d ms", tbl.ID, tbl.GetTableName(), e.Sub(s).Milliseconds())
		}
		return kvItems, nil
	case readop.RANGE:
		kvItems := make([]store.KVItem, 0, 10)
		log.Debugf("reading from key %v to %v", string(readOp.StartKey), string(readOp.EndKey))
		iter := tbl.leveldb.NewIterator(&util.Range{Start: readOp.StartKey, Limit: readOp.EndKey}, nil)
		skipFirst := readOp.SkipFirst
		for iter.Next() {
			if skipFirst {
				if utils.AreByteStringsSame(readOp.StartKey, iter.Key()) {
					skipFirst = false
					continue
				}
			}
			k := make([]byte, len(iter.Key()))
			v := make([]byte, len(iter.Value()))
			copy(k, iter.Key())
			copy(v, iter.Value())
			kvItems = append(kvItems, store.KVItem{
				Key:   k,
				Value: v,
			})
		}
		iter.Release()
		if !readOp.SkipLast {
			v, err := tbl.leveldb.Get(readOp.EndKey, nil)
			if err != nil {
				if err.Error() != "leveldb: not found" {
					log.Errorln(err)
					return nil, store.ErrStoreGetError
				}
			}
			if v != nil {
				kvItems = append(kvItems, store.KVItem{
					Key:   readOp.EndKey,
					Value: v,
				})
			}
		}
		e := time.Now()
		if e.Sub(s).Milliseconds() > 50 {
			log.Warningf("Node %v experienced a long range read on table %s. Range read took %d ms", tbl.ID, tbl.GetTableName(), e.Sub(s).Milliseconds())
		}
		return kvItems, nil
	case readop.PREFIX:
		kvItems := make([]store.KVItem, 0, 10)
		log.Debugf("Node %v reading prefix %v", tbl.ID, string(readOp.StartKey))
		//tstart := time.Now().UnixNano()
		iter := tbl.leveldb.NewIterator(util.BytesPrefix(readOp.StartKey), nil)
		for iter.Next() {
			k := make([]byte, len(iter.Key()))
			v := make([]byte, len(iter.Value()))
			copy(k, iter.Key())
			copy(v, iter.Value())
			kvItems = append(kvItems, store.KVItem{
				Key:   k,
				Value: v,
			})
		}
		iter.Release()
		//tdelta := (time.Now().UnixNano() - tstart) / 1000
		//log.Infof("Read from DB in %d microseconds", tdelta)
		log.Debugf("Node %v Read %d values from level db", tbl.ID, len(kvItems))
		e := time.Now()
		if e.Sub(s).Milliseconds() > 50 {
			log.Warningf("Node %v experienced a long prefix read on table %s. Prefix read took %d ms", tbl.ID, tbl.GetTableName(), e.Sub(s).Milliseconds())
		}
		return kvItems, nil
	}
	return nil, store.ErrStoreGetError
}

func (tbl *LevelDBKVTable) Write(payload *writeop.WriteOp) error {
	log.Debugf("Node %s applying WriteOp of %d items to KVTable\n", tbl.ID, len(payload.Items))
	s := time.Now()
	batch := new(leveldb.Batch)
	for _, item := range payload.Items {
		if item.Value == nil {
			batch.Delete(item.Key)
		} else {
			batch.Put(item.Key, item.Value)
		}
	}
	err := tbl.leveldb.Write(batch, nil)
	if err != nil {
		return err
	}
	e := time.Now()
	if e.Sub(s).Milliseconds() > 50 {
		log.Warningf("Node %v experienced a long write on table %s. Write took %d ms", tbl.ID, tbl.GetTableName(), e.Sub(s).Milliseconds())
	}
	return nil
}

func (tbl *LevelDBKVTable) SlowDeleteAllFound(deleteSpec *operations.DeleteOp) error {
	// again, we assume all concurrency control is handled in the upper layers, so nobody is trying to read/write the
	// partition specified in the read op in parallel with us
	// we perform slow delete by reading first, followed by the deleting each key we read
	items, err := tbl.Read(&deleteSpec.ReadSpec)
	if err != nil {
		return err
	}

	batch := new(leveldb.Batch)
	batch.Put(deleteSpec.VersionItem.Key, deleteSpec.VersionItem.Value)
	for _, item := range items {
		if item.Value == nil {
			batch.Delete(item.Key)
		}
	}
	err = tbl.leveldb.Write(batch, nil)
	if err != nil {
		return err
	}
	return nil
}

func (tbl *LevelDBKVTable) ReadConflictDomainVersion(cdkey store.ByteString) (uint64, error) {
	val, err := tbl.leveldb.Get(cdkey, nil)
	if err != nil {
		if err.Error() == "leveldb: not found" {
			return 0, store.ErrNotFound
		}
		log.Errorln(err)
		return 0, store.ErrStoreGetError
	}
	v := binary.BigEndian.Uint64(val)

	return v, nil
}

func (tbl *LevelDBKVTable) String() string {
	return fmt.Sprintf("KVTable on node %v. TableID: %v", tbl.ID, tbl.GetUUID())
}
