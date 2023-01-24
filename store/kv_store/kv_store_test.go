package kv_store

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/UNH-DistSyS/UNH-CLT/config"
	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/log"
	"github.com/UNH-DistSyS/UNH-CLT/store/kv_store/kv_table"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

var table kv_table.KVTable
var tempDir string
var tblId uuid.UUID

func cleanupTempDir(t *testing.T) {
	if table != nil {
		table.Close()
	}
	err := os.RemoveAll(tempDir)
	if err != nil {
		if t != nil {
			assert.Fail(t, "failed to cleanup temp dir", err)
		} else {
			log.Errorf("failed to cleanup temp dir")
		}
	}
}

func setupStore(t *testing.T, id ids.ID) *KVStore {
	tmpDir, err := ioutil.TempDir("", "kvdata")
	if err != nil {
		assert.Fail(t, "failed creating a temp dir")
	}
	tempDir = tmpDir
	scfg := config.NewDefaultStoreConfig()
	scfg.DBDir = tempDir
	store := NewStore(id, scfg, nil)
	return store
}

func TestKVStore_Initialize(t *testing.T) {
	id := *ids.GetIDFromString("1.1")
	store := setupStore(t, id)
	defer cleanupTempDir(t)

	store.Initialize()

	tables := store.GetTableList()

	log.Infof("Found tables: %v", tables)
	assert.Equal(t, 2, len(tables))
	// now add another table

	tblId := uuid.New()
	store.CreateTable("test_tbl1", tblId)

	tables = store.GetTableList()
	expectedTables := []string{"test_tbl1", config.LocusDBTablesTableName, config.LocusDBConflictDomainsTableName}

	log.Infof("Found tables: %v", tables)
	assert.Equal(t, 3, len(tables))
	for _, tblName := range tables {
		contains := false
		for _, expected := range expectedTables {
			if tblName == expected {
				contains = true
			}
		}
		assert.True(t, contains)
	}

	store.Close()
	//time.Sleep(100 * time.Millisecond)
	scfg := config.NewDefaultStoreConfig()
	scfg.DBDir = tempDir
	store2 := NewStore(id, scfg, nil)

	store2.Initialize()

	tables2 := store2.GetTableList()
	log.Infof("Found tables: %v", tables2)
	assert.Equal(t, 3, len(tables2))
	for _, tblName := range tables {
		contains := false
		for _, expected := range expectedTables {
			if tblName == expected {
				contains = true
			}
		}
		assert.True(t, contains)
	}
	store2.Close()

}
