package config

type StoreConfig struct {
	// Client Specific Configs
	DBDir                string `json:"db_dir"`             // directory for LevelDB instances
	StoreEngine          string `json:"store_engine"`       // store engine. Currently leveldb or badgerdb, inmemory
	BadgerDBInMemory     bool   `json:"badgerdb_in_memory"` // whether badgerdb runs in-memory only
	BadgerDBSyncWrites   bool   `json:"badgerdb_sync_writes"`
	BadgerDBValueLogSize int    `json:"badgerdb_value_log_size"`
	BTreeDegree          int    `json:"btree_degree"` // degree of b-tree when StoreEngine="inmemory"
}

func NewDefaultStoreConfig() *StoreConfig {
	config := new(StoreConfig)
	config.DBDir = "/tmp/lvldb/"
	config.StoreEngine = "leveldb"
	config.BadgerDBInMemory = false
	config.BadgerDBSyncWrites = true
	config.BadgerDBValueLogSize = 1024 * 1024 * 200 // 200 MBs
	config.BTreeDegree = 25

	return config
}
