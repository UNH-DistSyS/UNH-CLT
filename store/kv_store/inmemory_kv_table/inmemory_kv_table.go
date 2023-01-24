package inmemory_kv_table

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/UNH-DistSyS/UNH-CLT/config"
	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/log"
	"github.com/UNH-DistSyS/UNH-CLT/replication/operations"
	"github.com/UNH-DistSyS/UNH-CLT/replication/operations/readop"
	"github.com/UNH-DistSyS/UNH-CLT/replication/operations/writeop"
	"github.com/UNH-DistSyS/UNH-CLT/store"
	"github.com/google/btree"
	"github.com/google/uuid"
	"sync"
	"time"
)

// in-memory database maintains the key-value datastore in RAM without persistence

type InMemoryTable struct {
	ids.ID // must have an identity
	store.BaseTable
	*sync.RWMutex
	btree *btree.BTreeG[store.KVItem]
}

// NewInMemoryTable gets the instance of table B-tree backed in-memory table
func NewInMemoryTable(identity ids.ID,
	scfg *config.StoreConfig,
	name string,
	id uuid.UUID) *InMemoryTable {
	log.Infof("Creating In-Memory KV-table %v at node %v \n", name, identity)
	tbl := new(InMemoryTable)
	tbl.ID = identity
	tbl.BaseTable = store.NewBaseTable(name, id)
	tbl.RWMutex = new(sync.RWMutex)

	tbl.btree = btree.NewG[store.KVItem](scfg.BTreeDegree, func(itemA, itemB store.KVItem) bool {
		return bytes.Compare(itemA.Key, itemB.Key) < 0
	})
	return tbl
}

func (tbl *InMemoryTable) Close() {
	tbl.btree.Clear(false)
	// nothing
}

func (tbl *InMemoryTable) Size() int {
	return tbl.btree.Len()
}

// We generally do not wrap individual reads and writes into a transactions, since we control the concurrency
// at a higher level - Paxos and executor. So only one operation can access a partition at a time
func (tbl *InMemoryTable) Read(readOp *readop.ReadOp) ([]store.KVItem, error) {
	tbl.RLock()
	defer tbl.RUnlock()
	log.Debugf("Read against In-Memory on node %v. ReadMode=%v", tbl.ID, readOp.ReadMode)
	s := time.Now()
	switch readOp.ReadMode {
	case readop.POINT:
		dummyItem := store.KVItem{
			Key:   readOp.StartKey,
			Value: nil,
		}
		item, found := tbl.btree.Get(dummyItem)

		if !found {
			return nil, store.ErrNotFound
		}

		kvItems := []store.KVItem{item}
		e := time.Now()
		if e.Sub(s).Milliseconds() > 50 {
			log.Warningf("Node %v experienced a long point read on table %s. Point read took %d ms", tbl.ID, tbl.GetTableName(), e.Sub(s).Milliseconds())
		}
		return kvItems, nil
	case readop.RANGE:
		kvItems := make([]store.KVItem, 0, 10)
		start := store.KVItem{
			Key:   readOp.StartKey,
			Value: nil,
		}
		end := store.KVItem{
			Key:   readOp.EndKey,
			Value: nil,
		}
		log.Debugf("reading from key %v to %v", string(readOp.StartKey), string(readOp.EndKey))
		skipFirst := readOp.SkipFirst
		tbl.btree.AscendRange(start, end, func(item store.KVItem) bool {
			// TODO: check if we actually need these SkipFirst and SkipLast
			if skipFirst && bytes.Equal(item.Key, readOp.StartKey) {
				skipFirst = false
				return true
			}
			kvItems = append(kvItems, item)
			return true
		})

		if !readOp.SkipLast {
			dummyItem := store.KVItem{
				Key:   readOp.EndKey,
				Value: nil,
			}
			item, found := tbl.btree.Get(dummyItem)
			if found {
				kvItems = append(kvItems, item)
			}
		}

		e := time.Now()
		if e.Sub(s).Milliseconds() > 50 {
			log.Warningf("Node %v experienced a long range read on table %s. Range read took %d ms", tbl.ID, tbl.GetTableName(), e.Sub(s).Milliseconds())
		}
		return kvItems, nil
	case readop.PREFIX:
		kvItems := make([]store.KVItem, 0, 10)
		start := store.KVItem{
			Key:   readOp.StartKey,
			Value: nil,
		}

		tbl.btree.AscendGreaterOrEqual(start, func(item store.KVItem) bool {
			if !bytes.HasPrefix(item.Key, start.Key) {
				return false
			}
			kvItems = append(kvItems, item)
			return true
		})

		e := time.Now()
		if e.Sub(s).Milliseconds() > 50 {
			log.Warningf("Node %v experienced a long prefix read on table %s. Prefix read took %d ms", tbl.ID, tbl.GetTableName(), e.Sub(s).Milliseconds())
		}
		return kvItems, nil
	}
	return nil, store.ErrStoreGetError
}

func (tbl *InMemoryTable) Write(payload *writeop.WriteOp) error {
	tbl.Lock()
	defer tbl.Unlock()
	log.Debugf("Node %s applying WriteOp of %d items to KVTable\n", tbl.ID, len(payload.Items))
	s := time.Now()

	for _, item := range payload.Items {
		if item.Value == nil {
			tbl.btree.Delete(item)
		} else {
			tbl.btree.ReplaceOrInsert(item)
		}
	}

	e := time.Now()
	if e.Sub(s).Milliseconds() > 50 {
		log.Warningf("Node %v experienced a long write on table %s. Write took %d ms", tbl.ID, tbl.GetTableName(), e.Sub(s).Milliseconds())
	}
	return nil
}

func (tbl *InMemoryTable) SlowDeleteAllFound(deleteSpec *operations.DeleteOp) error {
	// again, we assume all concurrency control is handled in the upper layers, so nobody is trying to read/write the
	// partition specified in the read op in parallel with us
	// we perform slow delete by reading first, followed by the deleting each key we read
	items, err := tbl.Read(&deleteSpec.ReadSpec)
	if err != nil {
		return err
	}

	for _, item := range items {
		tbl.btree.Delete(item)
	}

	return nil
}

func (tbl *InMemoryTable) ReadConflictDomainVersion(cdkey store.ByteString) (uint64, error) {

	dummyItem := store.KVItem{
		Key:   cdkey,
		Value: nil,
	}
	cdkeyVersionItem, found := tbl.btree.Get(dummyItem)

	if !found {
		return 0, store.ErrNotFound
	}
	v := binary.BigEndian.Uint64(cdkeyVersionItem.Value)
	return v, nil
}

func (tbl *InMemoryTable) String() string {
	return fmt.Sprintf("In-Memory KVTable on node %v. TableID: %v", tbl.ID, tbl.GetUUID())
}
