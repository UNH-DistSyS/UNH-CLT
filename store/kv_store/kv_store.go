package kv_store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/UNH-DistSyS/UNH-CLT/store/kv_store/inmemory_kv_table"

	"github.com/UNH-DistSyS/UNH-CLT/config"
	"github.com/UNH-DistSyS/UNH-CLT/conflict_domain"
	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/log"
	"github.com/UNH-DistSyS/UNH-CLT/replication/operations"
	"github.com/UNH-DistSyS/UNH-CLT/replication/operations/readop"
	"github.com/UNH-DistSyS/UNH-CLT/replication/operations/writeop"
	"github.com/UNH-DistSyS/UNH-CLT/store"
	badgerdb_kv_table "github.com/UNH-DistSyS/UNH-CLT/store/kv_store/badger_kv_table"
	"github.com/UNH-DistSyS/UNH-CLT/store/kv_store/kv_table"
	"github.com/UNH-DistSyS/UNH-CLT/store/kv_store/leveldb_kv_table"
	"github.com/google/uuid"
)

// KVStore maintains the key-value datastore
type KVStore struct {
	ids.ID                 // must have an identity
	scfg                   *config.StoreConfig
	lock                   *sync.RWMutex
	tables                 map[uuid.UUID]kv_table.KVTable // map of leveldb by cql_store if
	tableNames             map[string]*uuid.UUID          // map of leveldb shards
	conflictDomainResolver conflict_domain.ConflictDomainResolver
	sync.RWMutex
}

// NewStore get the instance of storage component
func NewStore(identity ids.ID, scfg *config.StoreConfig, cdResolver conflict_domain.ConflictDomainResolver) *KVStore {
	log.Infof("Creating KV store at node %v \n", identity)
	kvstore := new(KVStore)
	kvstore.ID = identity
	kvstore.scfg = scfg
	kvstore.lock = new(sync.RWMutex)
	kvstore.tableNames = make(map[string]*uuid.UUID)
	kvstore.tables = make(map[uuid.UUID]kv_table.KVTable)
	kvstore.conflictDomainResolver = cdResolver
	return kvstore
}

func (s *KVStore) Initialize() error {
	// create tables table
	log.Infof("Initializing KV store at node %v", s.ID)
	_, err := s.CreateTable(config.LocusDBTablesTableName, config.LocusDBTablesUUID)
	if err != nil {
		return err
	}
	// create conflict domains table
	_, err = s.CreateTable(config.LocusDBConflictDomainsTableName, config.LocusDBConflictDomainsTableUUID)
	if err != nil {
		return err
	}

	// read tables table and load other tables
	readOp := &readop.ReadOp{
		ReadMode:  readop.PREFIX,
		SkipFirst: false,
		SkipLast:  false,
		StartKey:  nil,
		EndKey:    nil,
	}
	tablesTable := s.GetTable(config.LocusDBTablesUUID)
	kvItems, err := tablesTable.Read(readOp)
	if err != nil {
		log.Fatalf("Error initializing KVStore: %v", err)
		return err
	}

	for _, item := range kvItems {
		tableId, err := uuid.FromBytes(item.Key)
		if err != nil {
			log.Errorf("Error reading table (%v) tableId: %v", string(item.Value), item.Key)
			return err
		}
		_, err = s.CreateTable(string(item.Value), tableId)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *KVStore) Close() {
	s.Lock()
	defer s.Unlock()
	for _, tbl := range s.tables {
		tbl.Close()
		delete(s.tables, tbl.GetUUID())
	}
}

func (s *KVStore) GetConflictDomainResolver(cdStoreHeaderByte uint8) conflict_domain.ConflictDomainResolver {
	return s.conflictDomainResolver
}

func (s *KVStore) CloseTable(tableId uuid.UUID) {
	s.Lock()
	defer s.Unlock()
	if tbl, ok := s.tables[tableId]; ok {
		tbl.Close()
		delete(s.tables, tableId)
	}
}

func (s *KVStore) GetTableList() []string {
	tblList := make([]string, 0, len(s.tables))
	for _, tbl := range s.tables {
		tblList = append(tblList, tbl.GetTableName())
	}
	return tblList
}

func (s *KVStore) ResolveTableId(tableName string) *uuid.UUID {
	return s.tableNames[tableName]
}

func (s *KVStore) TableExists(tableId uuid.UUID) bool {
	return s.tables[tableId] != nil
}

func (s *KVStore) CreateTable(tableName string, tableId uuid.UUID) (bool, error) {
	s.Lock()
	defer s.Unlock()
	log.Debugf("KVStore %v creating table %s with id=%v", s.ID, tableName, tableId)
	if s.tables[tableId] == nil && s.tableNames[tableName] == nil {
		var tbl kv_table.KVTable
		switch s.scfg.StoreEngine {
		case "leveldb":
			tbl = leveldb_kv_table.NewLevelDBTable(s.ID, s.scfg.DBDir, tableName, tableId)
		case "badgerdb":
			tbl = badgerdb_kv_table.NewBadgerDBTable(s.ID, s.scfg, tableName, tableId)
		case "inmemory":
			tbl = inmemory_kv_table.NewInMemoryTable(s.ID, s.scfg, tableName, tableId)
		default:
			err := fmt.Errorf("unknonw engine type %s", s.scfg.StoreEngine)
			return false, err
		}
		s.tables[tableId] = tbl
		s.tableNames[tableName] = &tableId

		// write this table to fleet_table table
		tablesTable := s.tables[config.LocusDBTablesUUID]
		tblIdBytes, err := tableId.MarshalBinary()
		if err != nil {
			log.Errorf("Error marshalling uuid to []bytes on table creation")
			return false, err
		} else {
			writeOp := &writeop.WriteOp{Items: []store.KVItem{{Key: store.ByteString(tblIdBytes), Value: store.ByteString(tableName)}}}
			err := tablesTable.Write(writeOp)
			if err != nil {
				return false, err
			}
		}
	} else {
		log.Infof("KVStore %v: table with this name (%s) or id (%v) already exists, skipping CreateTable", s.ID, tableName, tableId)
		return false, nil
	}
	return true, nil
}

func (s *KVStore) GetTable(id uuid.UUID) kv_table.KVTable {
	s.RLock()
	defer s.RUnlock()
	return s.tables[id]
}

func (s *KVStore) GetTableByName(name string) kv_table.KVTable {
	s.RLock()
	defer s.RUnlock()
	return s.tables[*s.tableNames[name]]
}

func (s *KVStore) ApplyCommand(command *store.Command, shortNonMutating bool) (*store.CommandResult, error) {
	switch command.OperationType {
	case store.WRITE:
		switch command.Operation.(type) {
		case writeop.WriteOp:
			writeOp := command.Operation.(writeop.WriteOp)
			tbl := s.GetTable(writeOp.TableUUID)
			if tbl == nil {
				return nil, store.ErrTableNotFound
			}

			if command.ConflictDomainKey != nil {

				if command.Version == 0 {
					return nil, fmt.Errorf("versioned store cannot have unversioned write")
				}

				b := make([]byte, 8)
				binary.BigEndian.PutUint64(b, command.Version)
				versionItem := store.KVItem{Key: command.ConflictDomainKey, Value: b}
				writeOp.Items = append(writeOp.Items, versionItem)
			}

			err := tbl.Write(&writeOp)
			if err != nil {
				return nil, err
			}
			return command.CreateResult(nil, writeOp.TableUUID), nil
		default:
			return nil, store.ErrIncorrectReplicationOp
		}
	case store.READ:
		switch command.Operation.(type) {
		case readop.ReadOp:
			if shortNonMutating {
				// this is a read-only operation that does not return to client, so we do not actually have to do the read
				return nil, nil
			}
			readOp := command.Operation.(readop.ReadOp)
			tbl := s.GetTable(readOp.TableUUID)
			if tbl == nil {
				return nil, store.ErrTableNotFound
			}
			kvItems, err := tbl.Read(&readOp)
			if err != nil {
				return nil, err
			}
			if command.Version == 0 && len(kvItems) > 0 && len(command.ConflictDomainKey) > 0 {
				command.Version, err = s.ReadVersionOfStoredConflictDomain(command.ConflictDomainKey)
				if err != nil {
					return nil, err
				}
			}
			return command.CreateResult(kvItems, readOp.TableUUID), nil
		default:
			return nil, store.ErrIncorrectReplicationOp
		}
	case store.DELETE:
		switch command.Operation.(type) {
		case readop.ReadOp:
			readOp := command.Operation.(readop.ReadOp)
			tbl := s.GetTable(readOp.TableUUID)
			if tbl == nil {
				return nil, store.ErrTableNotFound
			}

			deleteSpec := &operations.DeleteOp{
				ReadSpec:    readOp,
				ColumnNames: nil,
			}

			if command.ConflictDomainKey != nil {

				if command.Version == 0 {
					return nil, fmt.Errorf("versioned store cannot have unversioned write")
				}

				b := make([]byte, 8)
				binary.BigEndian.PutUint64(b, command.Version)
				versionItem := store.KVItem{Key: command.ConflictDomainKey, Value: b}
				deleteSpec.VersionItem = versionItem
			}

			err := tbl.SlowDeleteAllFound(deleteSpec)
			if err != nil {
				return nil, err
			}
			return command.CreateResult(nil, readOp.TableUUID), nil
		case operations.DeleteOp:
			deleteOp := command.Operation.(operations.DeleteOp)
			tbl := s.GetTable(deleteOp.ReadSpec.TableUUID)
			if tbl == nil {
				return nil, store.ErrTableNotFound
			}

			if command.ConflictDomainKey != nil {

				if command.Version == 0 {
					return nil, fmt.Errorf("versioned store cannot have unversioned write")
				}

				b := make([]byte, 8)
				binary.BigEndian.PutUint64(b, command.Version)
				versionItem := store.KVItem{Key: command.ConflictDomainKey, Value: b}
				deleteOp.VersionItem = versionItem
			}

			err := tbl.SlowDeleteAllFound(&deleteOp)
			if err != nil {
				return nil, err
			}
			return command.CreateResult(nil, deleteOp.ReadSpec.TableUUID), nil
		default:
			return nil, store.ErrIncorrectReplicationOp
		}
	case store.DDL_WRITE:
		switch command.Operation.(type) {
		case store.KVTableSpec:
			kvTableSpec := command.Operation.(store.KVTableSpec)
			_, err := s.CreateTable(kvTableSpec.TableName, kvTableSpec.TableUUID)
			return nil, err
		default:
			return nil, store.ErrIncorrectReplicationOp
		}
	case store.NOOP:
		return nil, nil

	default:
		return nil, store.ErrStoreError
	}
}

func (s *KVStore) String() string {
	return "LevelDB Database"
}

func (s *KVStore) ReadVersionOfStoredConflictDomain(conflictDomainKey store.ByteString) (uint64, error) {
	cdResolver := s.GetConflictDomainResolver(conflictDomainKey[0])
	if cdResolver == nil {
		return 0, errors.New("conflict domain resolver for this conflict domain is missing. consider upper level store")
	}

	tblUUIDs, err := cdResolver.ConflictDomainKeyToTableUUIDs(conflictDomainKey)
	if err != nil {
		return 0, err
	}
	maxversion := uint64(0)
	for _, tblUUID := range tblUUIDs {
		// directly access each table's CDVersion record
		tbl := s.GetTable(tblUUID)
		if tbl == nil {
			return 0, store.ErrTableNotFound
		}
		cdVersion, err := tbl.ReadConflictDomainVersion(conflictDomainKey)
		if err != nil {
			return 0, err
		}
		if maxversion < cdVersion {
			maxversion = cdVersion
		}
	}
	return maxversion, err
}
