package cql_store

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/UNH-DistSyS/UNH-CLT/config"
	"github.com/UNH-DistSyS/UNH-CLT/conflict_domain"
	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/log"
	"github.com/UNH-DistSyS/UNH-CLT/replication/operations"
	"github.com/UNH-DistSyS/UNH-CLT/replication/operations/readop"
	"github.com/UNH-DistSyS/UNH-CLT/replication/operations/writeop"
	"github.com/UNH-DistSyS/UNH-CLT/store"
	"github.com/UNH-DistSyS/UNH-CLT/store/cql_store/cql_data_model"
	"github.com/UNH-DistSyS/UNH-CLT/store/kv_store"
	"github.com/UNH-DistSyS/UNH-CLT/utils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

//var table *KVTable
var tempDir string
var tblId uuid.UUID

func cleanupTempDir(t *testing.T) {
	err := os.RemoveAll(tempDir)
	if err != nil {
		assert.Fail(t, "failed to cleanup a temp dir")
	}
}

func setupStore(t *testing.T, id ids.ID) *CQLStore {
	tmpDir, err := ioutil.TempDir("", "kvdata")
	if err != nil {
		assert.Fail(t, "failed creating a temp dir")
	}
	tempDir = tmpDir
	scfg := config.NewDefaultStoreConfig()
	scfg.DBDir = tempDir
	kvstore := kv_store.NewStore(id, scfg, nil)
	cqlCDResolver := conflict_domain.NewCQLConflictDomainResolver()
	store := NewCQLStore(kvstore, cqlCDResolver)
	return store
}

func TestCQLStore_Initialize(t *testing.T) {
	id := *ids.GetIDFromString("1.1")
	store := setupStore(t, id)
	defer cleanupTempDir(t)

	store.Initialize()

	tables := store.GetTableList()

	log.Infof("Found tables: %v", tables)
	expectedTables := []string{"locus_columns", config.LocusDBTablesTableName, config.LocusDBConflictDomainsTableName}
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
}

func TestCQLStore_createTable(t *testing.T) {
	id := *ids.GetIDFromString("1.1")
	store := setupStore(t, id)
	defer cleanupTempDir(t)
	store.Initialize()

	testTblSpec := getOnePKThreeColsTableSpecs()
	err := store.createCQLTable(testTblSpec, 1)

	assert.Nil(t, err)

	tables := store.GetTableList()
	expectedTables := []string{testTblSpec.TableName, "locus_columns", config.LocusDBTablesTableName, config.LocusDBConflictDomainsTableName}
	log.Infof("Found tables: %v", tables)
	assert.Equal(t, 4, len(tables))
	for _, tblName := range tables {
		contains := false
		for _, expected := range expectedTables {
			if tblName == expected {
				contains = true
			}
		}
		assert.True(t, contains)
	}

	// now check if CQL table is in memory
	tbl := store.GetTable(testTblSpec.TableUUID)
	assert.NotNil(t, tbl)
	assert.Equal(t, len(testTblSpec.ColumnSpecs), len(tbl.tableSpec.ColumnSpecs))

	// now check if CQL table is on disk
	store.Close()
	// reopen a store from the same tempDir
	scfg := config.NewDefaultStoreConfig()
	scfg.DBDir = tempDir
	kvstore := kv_store.NewStore(id, scfg, nil)
	store2 := NewCQLStore(kvstore, nil)
	store2.Initialize()

	// all tables should show up after loading from disk
	tables = store2.GetTableList()
	expectedTables = []string{testTblSpec.TableName, "locus_columns", config.LocusDBTablesTableName, config.LocusDBConflictDomainsTableName}
	log.Infof("Found tables: %v", tables)
	assert.Equal(t, 4, len(tables))
	for _, tblName := range tables {
		contains := false
		for _, expected := range expectedTables {
			if tblName == expected {
				contains = true
			}
		}
		assert.True(t, contains)
	}

	// and our created table should have its spec
	tbl = store2.GetTable(testTblSpec.TableUUID)
	assert.NotNil(t, tbl)
	assert.Equal(t, len(testTblSpec.ColumnSpecs), len(tbl.tableSpec.ColumnSpecs))
	tbl2 := store2.GetTableByName(testTblSpec.TableName)
	assert.NotNil(t, tbl2)
	assert.Equal(t, len(testTblSpec.ColumnSpecs), len(tbl2.tableSpec.ColumnSpecs))

	for _, expectedSpec := range testTblSpec.ColumnSpecs {
		loadedSpec := tbl2.tableSpec.GetColumnSpecByName(expectedSpec.Name)
		assert.Equal(t, expectedSpec.Name, loadedSpec.Name)
		assert.Equal(t, expectedSpec.DataType, loadedSpec.DataType)
		assert.Equal(t, expectedSpec.Kind, loadedSpec.Kind)
		assert.Equal(t, expectedSpec.OrderNum, loadedSpec.OrderNum)
		assert.Equal(t, expectedSpec.Id, loadedSpec.Id)
	}

	store2.Close()
}

func TestCQLStore_ApplyCommand_SimpleWriteAndRead(t *testing.T) {
	id := *ids.GetIDFromString("1.1")
	cqlStore := setupStore(t, id)
	defer cleanupTempDir(t)
	cqlStore.Initialize()

	testTblSpec := getOnePKThreeColsTableSpecs()
	cqlStore.createCQLTable(testTblSpec, 1)

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	row := make(map[uint8]cql_data_model.LocusDBValue)
	row[0] = cql_data_model.NewInt(42 + int32(rnd.Intn(100)))        // pk
	row[1] = cql_data_model.NewInt(42 + int32(rnd.Intn(100)))        // ck
	row[2] = cql_data_model.NewInt(42 + int32(rnd.Intn(100)))        // c1
	row[3] = cql_data_model.NewFloat(42.15 + float32(rnd.Intn(100))) // c2
	row[4] = cql_data_model.NewTextValue(utils.RandString(10, rnd))  // c3

	testTbl := cqlStore.GetTable(testTblSpec.TableUUID)
	items, primaryKey, err := testTbl.RowToKVItems(row)
	assert.Nil(t, err)
	var cdKey []byte
	cdKey, err = cqlStore.conflictDomainResolver.KeyToConflictDomain(primaryKey, testTbl.tableSpec.TableUUID)
	assert.Nil(t, err)

	payload := writeop.WriteOp{
		TableUUID: testTblSpec.TableUUID,
		Items:     items,
	}

	writeVersion := uint64(17)

	cmd := store.Command{
		ConflictDomainKey: cdKey,
		CommandID:         ids.CommandID(42),
		OperationType:     store.WRITE,
		Operation:         payload,
		TargetStore:       store.CQL,
		Version:           writeVersion,
	}

	cr, err := cqlStore.ApplyCommand(&cmd, false)
	if err != nil {
		assert.Fail(t, fmt.Sprintf("Received Error from the store: %v", err))
	}
	assert.NotNil(t, cr)
	assert.Equal(t, ids.CommandID(42), cr.CommandID)

	// now construct a version-less read command for the row

	readOp := &readop.ReadOp{StartKey: primaryKey, EndKey: primaryKey, ReadMode: readop.PREFIX, SkipFirst: false, SkipLast: false, TableUUID: testTblSpec.TableUUID}
	cqlReadOp := readop.CQLReadOp{
		ReadOp: readOp,
		ColIds: []uint8{0, 1, 2, 3, 4},
	}
	//replyChan := make(chan request.ReadReply, 1)
	readCmd := store.Command{
		Operation:     cqlReadOp,
		OperationType: store.READ,
		CommandID:     42,
		//TableUUID:           tableIdForPaxosTests,
		ConflictDomainKey: cdKey,
		TargetStore:       store.CQL,
	}

	cr, err = cqlStore.ApplyCommand(&readCmd, false)
	if err != nil {
		assert.Fail(t, fmt.Sprintf("Received Error from the store: %v", err))
	}
	assert.NotNil(t, cr)
	assert.Equal(t, writeVersion, cr.Version)
	assert.NotNil(t, cr.Result)
	tblSnip := cr.Result.(*TableSnippet)
	assert.NotNil(t, tblSnip)
	assert.Equal(t, 1, len(tblSnip.Rows))
	assert.Equal(t, row[0], tblSnip.Rows[0][0])
	assert.Equal(t, row[1], tblSnip.Rows[0][1])
	assert.Equal(t, row[2], tblSnip.Rows[0][2])
	assert.Equal(t, row[3], tblSnip.Rows[0][3])
	assert.Equal(t, row[4], tblSnip.Rows[0][4])

	cqlStore.Close()
}

func TestCQLStore_ApplyCommand_PartitionUpdate(t *testing.T) {
	id := *ids.GetIDFromString("1.1")
	cqlStore := setupStore(t, id)
	defer cleanupTempDir(t)
	cqlStore.Initialize()

	testTblSpec := getOnePKThreeColsTableSpecs()
	cqlStore.createCQLTable(testTblSpec, 1)
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	// first write some row to the same partitions
	numRows := 2
	rows := make([]cql_data_model.Row, numRows)
	var primaryKey store.ByteString
	var cdKey []byte
	writeVersion := uint64(17)
	for rowNum := 0; rowNum < numRows; rowNum++ {
		row := make(map[uint8]cql_data_model.LocusDBValue)
		row[0] = cql_data_model.NewInt(42)                               // pk
		row[1] = cql_data_model.NewInt(43 + int32(rowNum))               // ck
		row[2] = cql_data_model.NewInt(42 + int32(rnd.Intn(100)))        // c1
		row[3] = cql_data_model.NewFloat(42.15 + float32(rnd.Intn(100))) // c2
		row[4] = cql_data_model.NewTextValue(utils.RandString(10, rnd))  // c3
		rows[rowNum] = row

		testTbl := cqlStore.GetTable(testTblSpec.TableUUID)
		var items []store.KVItem
		var err error
		items, primaryKey, err = testTbl.RowToKVItems(row)
		assert.Nil(t, err)
		cdKey, err = cqlStore.conflictDomainResolver.KeyToConflictDomain(primaryKey, testTbl.tableSpec.TableUUID)
		assert.Nil(t, err)

		payload := writeop.WriteOp{
			TableUUID: testTblSpec.TableUUID,
			Items:     items,
		}

		writeVersion++

		cmd := store.Command{
			ConflictDomainKey: cdKey,
			CommandID:         ids.CommandID(42 + rowNum),
			OperationType:     store.WRITE,
			Operation:         payload,
			TargetStore:       store.CQL,
			Version:           writeVersion,
		}

		cr, err := cqlStore.ApplyCommand(&cmd, false)
		if err != nil {
			assert.Fail(t, fmt.Sprintf("Received Error from the store: %v", err))
		}
		assert.NotNil(t, cr)
		assert.Equal(t, ids.CommandID(42+rowNum), cr.CommandID)
	}

	// now lets do a partition update
	row := make(map[uint8]cql_data_model.LocusDBValue)
	row[0] = cql_data_model.NewInt(42)  // pk
	row[2] = cql_data_model.NewInt(242) // c1

	readOp := &readop.ReadOp{StartKey: primaryKey, EndKey: primaryKey, ReadMode: readop.PREFIX, SkipFirst: false, SkipLast: false, TableUUID: testTblSpec.TableUUID}
	cqlReadOp := readop.CQLReadOp{
		ReadOp: readOp,
		ColIds: []uint8{0, 1, 2, 3, 4},
	}
	payload := operations.CqlUpdateOp{
		WriteRow: row,
		ReadSpec: cqlReadOp,
	}

	writeVersion++

	cmd := store.Command{
		ConflictDomainKey: cdKey,
		CommandID:         ids.CommandID(342),
		OperationType:     store.UPDATE,
		Operation:         payload,
		TargetStore:       store.CQL,
		Version:           writeVersion,
	}

	cr, err := cqlStore.ApplyCommand(&cmd, false)
	if err != nil {
		assert.Fail(t, fmt.Sprintf("Received Error from the store: %v", err))
	}
	assert.NotNil(t, cr)
	assert.Equal(t, ids.CommandID(342), cr.CommandID)

	// now do a partition read

	readCmd := store.Command{
		Operation:         cqlReadOp,
		OperationType:     store.READ,
		CommandID:         42,
		ConflictDomainKey: cdKey,
		TargetStore:       store.CQL,
	}

	cr, err = cqlStore.ApplyCommand(&readCmd, false)
	if err != nil {
		assert.Fail(t, fmt.Sprintf("Received Error from the store: %v", err))
	}
	assert.NotNil(t, cr)
	assert.Equal(t, writeVersion, cr.Version)
	assert.NotNil(t, cr.Result)
	tblSnip := cr.Result.(*TableSnippet)
	assert.NotNil(t, tblSnip)
	assert.Equal(t, 2, len(tblSnip.Rows))
	for i := 0; i < len(rows); i++ {
		assert.Equal(t, rows[i][0], tblSnip.Rows[0][0])
		assert.Equal(t, rows[i][1], tblSnip.Rows[i][1])
		assert.Equal(t, "242", tblSnip.Rows[i][2].String())
		assert.Equal(t, rows[i][3], tblSnip.Rows[i][3])
		assert.Equal(t, rows[i][4], tblSnip.Rows[i][4])
	}

	cqlStore.Close()
}

func TestCQLStore_ApplyCommand_PartitionUpdateWithExpression(t *testing.T) {
	id := *ids.GetIDFromString("1.1")
	cqlStore := setupStore(t, id)
	defer cleanupTempDir(t)
	cqlStore.Initialize()

	testTblSpec := getOnePKThreeColsTableSpecs()
	cqlStore.createCQLTable(testTblSpec, 1)
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	// first write some row to the same partitions
	numRows := 2
	rows := make([]cql_data_model.Row, numRows)
	var primaryKey store.ByteString
	var cdKey []byte
	writeVersion := uint64(17)
	for rowNum := 0; rowNum < numRows; rowNum++ {
		row := make(map[uint8]cql_data_model.LocusDBValue)
		row[0] = cql_data_model.NewInt(42)                               // pk
		row[1] = cql_data_model.NewInt(43 + int32(rowNum))               // ck
		row[2] = cql_data_model.NewInt(42 + int32(rnd.Intn(100)))        // c1
		row[3] = cql_data_model.NewFloat(42.15 + float32(rnd.Intn(100))) // c2
		row[4] = cql_data_model.NewTextValue(utils.RandString(10, rnd))  // c3
		rows[rowNum] = row

		testTbl := cqlStore.GetTable(testTblSpec.TableUUID)
		var items []store.KVItem
		var err error
		items, primaryKey, err = testTbl.RowToKVItems(row)
		assert.Nil(t, err)

		payload := writeop.WriteOp{
			TableUUID: testTblSpec.TableUUID,
			Items:     items,
		}

		cdKey, err := cqlStore.conflictDomainResolver.KeyToConflictDomain(primaryKey, testTblSpec.TableUUID)
		assert.Nil(t, err)

		writeVersion++

		cmd := store.Command{
			ConflictDomainKey: cdKey,
			CommandID:         ids.CommandID(42 + rowNum),
			OperationType:     store.WRITE,
			Operation:         payload,
			TargetStore:       store.CQL,
			Version:           writeVersion,
		}

		cr, err := cqlStore.ApplyCommand(&cmd, false)
		if err != nil {
			assert.Fail(t, fmt.Sprintf("Received Error from the store: %v", err))
		}
		assert.NotNil(t, cr)
		assert.Equal(t, ids.CommandID(42+rowNum), cr.CommandID)
	}

	// now lets do a partition update
	row := make(map[uint8]cql_data_model.LocusDBValue)
	row[0] = cql_data_model.NewInt(42)                   // pk
	row[2] = cql_data_model.NewExpressionValue("c1 + 2") // c1

	readOp := &readop.ReadOp{StartKey: primaryKey, EndKey: primaryKey, ReadMode: readop.PREFIX, SkipFirst: false, SkipLast: false, TableUUID: testTblSpec.TableUUID}
	cqlReadOp := readop.CQLReadOp{
		ReadOp: readOp,
		ColIds: []uint8{0, 1, 2, 3, 4},
	}

	payload := operations.CqlUpdateOp{
		WriteRow: row,
		ReadSpec: cqlReadOp,
	}

	writeVersion++
	cdKey, err := cqlStore.conflictDomainResolver.KeyToConflictDomain(primaryKey, testTblSpec.TableUUID)
	cmd := store.Command{
		ConflictDomainKey: cdKey,
		CommandID:         ids.CommandID(342),
		OperationType:     store.UPDATE,
		Operation:         payload,
		TargetStore:       store.CQL,
		Version:           writeVersion,
	}

	cr, err := cqlStore.ApplyCommand(&cmd, false)
	if err != nil {
		assert.Fail(t, fmt.Sprintf("Received Error from the store: %v", err))
	}
	assert.NotNil(t, cr)
	assert.Equal(t, ids.CommandID(342), cr.CommandID)

	// now do a partition read

	readCmd := store.Command{
		Operation:         cqlReadOp,
		OperationType:     store.READ,
		CommandID:         45,
		ConflictDomainKey: cdKey,
		TargetStore:       store.CQL,
	}

	cr, err = cqlStore.ApplyCommand(&readCmd, false)
	if err != nil {
		assert.Fail(t, fmt.Sprintf("Received Error from the store: %v", err))
	}
	assert.NotNil(t, cr)
	assert.Equal(t, writeVersion, cr.Version)
	assert.NotNil(t, cr.Result)
	tblSnip := cr.Result.(*TableSnippet)
	assert.NotNil(t, tblSnip)
	assert.Equal(t, 2, len(tblSnip.Rows))
	for i := 0; i < len(rows); i++ {
		updatedC2 := rows[i][2].(*cql_data_model.IntValue).Val + 2
		assert.Equal(t, rows[i][0], tblSnip.Rows[0][0])
		assert.Equal(t, rows[i][1], tblSnip.Rows[i][1])
		assert.Equal(t, cql_data_model.NewInt(updatedC2), tblSnip.Rows[i][2])
		assert.Equal(t, rows[i][3], tblSnip.Rows[i][3])
		assert.Equal(t, rows[i][4], tblSnip.Rows[i][4])
	}

	cqlStore.Close()
}
