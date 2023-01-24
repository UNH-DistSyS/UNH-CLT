package badgerdb_kv_table

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/UNH-DistSyS/UNH-CLT/config"
	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/log"
	"github.com/UNH-DistSyS/UNH-CLT/replication/operations"
	"github.com/UNH-DistSyS/UNH-CLT/replication/operations/readop"
	"github.com/UNH-DistSyS/UNH-CLT/replication/operations/writeop"
	"github.com/UNH-DistSyS/UNH-CLT/store"
	"github.com/dgraph-io/badger/v3"
	"github.com/google/uuid"
)

// leveldb database maintains the key-value datastore

type BadgerKVTable struct {
	ids.ID // must have an identity
	store.BaseTable
	*sync.RWMutex
	badgerdb *badger.DB
}

// NewTable get the instance of table LevelDB Wrapper
func NewBadgerDBTable(identity ids.ID,
	scfg *config.StoreConfig,
	name string,
	id uuid.UUID) *BadgerKVTable {
	log.Infof("Creating BadgerDB KV-table %v at node %v \n", name, identity)
	tbl := new(BadgerKVTable)
	tbl.ID = identity
	tbl.BaseTable = store.NewBaseTable(name, id)
	tbl.RWMutex = new(sync.RWMutex)
	var db *badger.DB
	var err error
	if scfg.BadgerDBInMemory {
		db, err = badger.Open(badger.DefaultOptions("").WithInMemory(true))
	} else {
		dbname := scfg.DBDir + "/" + identity.String() + "/" + id.String() + "/" + name
		badgerOptions := badger.DefaultOptions(dbname).WithValueLogFileSize(int64(scfg.BadgerDBValueLogSize)).WithSyncWrites(scfg.BadgerDBSyncWrites).WithDetectConflicts(false)
		//badgerOptions = badgerOptions.WithCompression(options.None)
		db, err = badger.Open(badgerOptions)
	}
	if err != nil {
		log.Errorf("Could not create badgerDB storage for KVTable %s: %v", name, err)
		return nil
	}
	tbl.badgerdb = db

	return tbl
}

func (tbl *BadgerKVTable) Close() {
	tbl.badgerdb.Close()
}

func (tbl *BadgerKVTable) Size() int {
	return 0
}

func (tbl *BadgerKVTable) Read(readOp *readop.ReadOp) ([]store.KVItem, error) {
	log.Debugf("Read against BadgerDB on node %v. ReadMode=%v", tbl.ID, readOp.ReadMode)
	s := time.Now()
	switch readOp.ReadMode {
	case readop.POINT:
		var kvItems []store.KVItem
		err := tbl.badgerdb.View(func(txn *badger.Txn) error {
			item, err := txn.Get(readOp.StartKey)
			if err != nil {
				if err.Error() == "badgerdb: not found" {
					return store.ErrNotFound
				}
				log.Errorln(err)
				return store.ErrStoreGetError
			}
			err = item.Value(func(v []byte) error {
				k := make([]byte, item.KeySize())
				kvItem := store.KVItem{
					Key:   item.KeyCopy(k),
					Value: v,
				}
				kvItems = append(kvItems, kvItem)
				return nil
			})

			if err != nil {
				return err
			}

			return nil
		})
		e := time.Now()
		if e.Sub(s).Milliseconds() > 5 {
			log.Warningf("Node %v experienced a long point read on table %s. Read took %d ms", tbl.ID, tbl.GetTableName(), e.Sub(s).Milliseconds())
		}
		return kvItems, err
	case readop.RANGE:
		kvItems := make([]store.KVItem, 0, 10)
		err := tbl.badgerdb.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			//opts.PrefetchSize = 2
			opts.PrefetchValues = false
			it := txn.NewIterator(opts)
			defer it.Close()
			skipFirst := readOp.SkipFirst
			for it.Seek(readOp.StartKey); it.Valid(); it.Next() {
				item := it.Item()

				// TODO: check if we actually need these SkipFirst and SkipLast
				bytesCmp := bytes.Compare(item.Key(), readOp.EndKey)
				if bytesCmp > 0 || readOp.SkipLast && bytesCmp == 0 {
					continue
				}
				if skipFirst && bytes.Equal(item.Key(), readOp.StartKey) {
					skipFirst = false
					continue
				}
				err := item.Value(func(v []byte) error {
					k := make([]byte, item.KeySize())
					kvItem := store.KVItem{
						Key:   item.KeyCopy(k),
						Value: v,
					}
					kvItems = append(kvItems, kvItem)
					return nil
				})

				if err != nil {
					return err
				}
			}
			return nil
		})

		e := time.Now()
		if e.Sub(s).Milliseconds() > 5 {
			log.Warningf("Node %v experienced a long ragne read on table %s. Read took %d ms", tbl.ID, tbl.GetTableName(), e.Sub(s).Milliseconds())
		}

		return kvItems, err
	case readop.PREFIX:
		kvItems := make([]store.KVItem, 0, 10)
		err := tbl.badgerdb.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchSize = 2
			opts.PrefetchValues = false
			opts.Prefix = readOp.StartKey
			it := txn.NewIterator(opts)
			defer it.Close()
			for it.Seek(readOp.StartKey); it.ValidForPrefix(readOp.StartKey); it.Next() {
				item := it.Item()
				err := item.Value(func(v []byte) error {
					k := make([]byte, item.KeySize())
					kvItem := store.KVItem{
						Key:   item.KeyCopy(k),
						Value: v,
					}
					kvItems = append(kvItems, kvItem)

					return nil
				})

				if err != nil {
					return err
				}
			}
			return nil
		})

		e := time.Now()
		if e.Sub(s).Milliseconds() > 5 {
			log.Warningf("Node %v experienced a long prefix read on table %s. Read took %d ms", tbl.ID, tbl.GetTableName(), e.Sub(s).Milliseconds())
		}
		return kvItems, err
	}
	return nil, store.ErrStoreGetError
}

func (tbl *BadgerKVTable) Write(payload *writeop.WriteOp) error {
	log.Debugf("Node %s applying WriteOp of %d items to KVTable\n", tbl.ID, len(payload.Items))
	wb := tbl.badgerdb.NewWriteBatch()
	defer wb.Cancel()

	s := time.Now()
	for _, item := range payload.Items {
		if item.Value == nil {
			err := wb.Delete(item.Key) // Will create txns as needed.
			if err != nil {
				return err
			}
		} else {
			err := wb.Set(item.Key, item.Value) // Will create txns as needed.
			if err != nil {
				return err
			}
		}
	}

	err := wb.Flush()
	if err != nil {
		return err
	}

	e := time.Now()
	if e.Sub(s).Milliseconds() > 50 {
		log.Warningf("Node %v experienced a long write on table %s. Write took %d ms", tbl.ID, tbl.GetTableName(), e.Sub(s).Milliseconds())
	}
	return nil
}

func (tbl *BadgerKVTable) SlowDeleteAllFound(deleteSpec *operations.DeleteOp) error {
	// we perform slow delete by reading first, followed by the deleting each key we read
	items, err := tbl.Read(&deleteSpec.ReadSpec)
	if err != nil {
		return err
	}

	wb := tbl.badgerdb.NewWriteBatch()
	defer wb.Cancel()
	err = wb.Set(deleteSpec.VersionItem.Key, deleteSpec.VersionItem.Value)
	if err != nil {
		return err
	}

	for _, item := range items {
		if item.Value == nil {
			err = wb.Delete(item.Key)
			if err != nil {
				return err
			}
		}
	}
	err = wb.Flush()
	if err != nil {
		return err
	}
	return nil
}

func (tbl *BadgerKVTable) ReadConflictDomainVersion(cdkey store.ByteString) (uint64, error) {

	var CDVersion uint64
	err := tbl.badgerdb.View(func(txn *badger.Txn) error {
		item, err := txn.Get(cdkey)
		if err != nil {
			if err.Error() == "badgerdb: not found" {
				return store.ErrNotFound
			}
			log.Errorln(err)
			return store.ErrStoreGetError
		}

		err = item.Value(func(v []byte) error {
			CDVersion = binary.BigEndian.Uint64(v)
			return nil
		})

		if err != nil {
			return err
		}
		return nil
	})

	return CDVersion, err
}

func (tbl *BadgerKVTable) String() string {
	return fmt.Sprintf("KVTable on node %v. TableID: %v", tbl.ID, tbl.GetUUID())
}
