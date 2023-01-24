package cql_request_handler

import (
	"io/ioutil"
	"testing"

	"github.com/UNH-DistSyS/UNH-CLT/config"
	"github.com/UNH-DistSyS/UNH-CLT/conflict_domain"
	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/log"
	"github.com/UNH-DistSyS/UNH-CLT/parsers/cql_parser"
	"github.com/UNH-DistSyS/UNH-CLT/replication/operations/readop"
	"github.com/UNH-DistSyS/UNH-CLT/replication/operations/writeop"
	"github.com/UNH-DistSyS/UNH-CLT/request"
	"github.com/UNH-DistSyS/UNH-CLT/store"
	"github.com/UNH-DistSyS/UNH-CLT/store/cql_store"
	"github.com/UNH-DistSyS/UNH-CLT/store/cql_store/cql_data_model"
	"github.com/UNH-DistSyS/UNH-CLT/store/kv_store"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

var testStoreTempDir string
var cfg = config.MakeDefaultConfig()

func setupStore(id ids.ID) *cql_store.CQLStore {
	tmpDir, err := ioutil.TempDir("", "kvdata")
	if err != nil {
		return nil
	}
	testStoreTempDir = tmpDir
	kvCDResolver := conflict_domain.NewKVConflictDomainResolver(conflict_domain.KVKeyConflictDomain)
	cqlCDResolver := conflict_domain.NewCQLConflictDomainResolver()
	scfg := config.NewDefaultStoreConfig()
	scfg.DBDir = tmpDir
	kvstore := kv_store.NewStore(id, scfg, kvCDResolver)
	store := cql_store.NewCQLStore(kvstore, cqlCDResolver)
	return store
}

func getSampleTableSpec() *cql_store.LocusDBTableSpec {
	var testColId = cql_store.NewLocusDBColumnSpec("id", cql_data_model.Text, 0, 0, cql_store.PartitionColumn)
	var testColCluster = cql_store.NewLocusDBColumnSpec("colcluster", cql_data_model.Text, 1, 1, cql_store.ClusteringColumn)
	var testCol1 = cql_store.NewLocusDBColumnSpec("col1", cql_data_model.Text, 2, 2, cql_store.RegularColumn)
	var testCol2 = cql_store.NewLocusDBColumnSpec("col2", cql_data_model.Int, 3, 3, cql_store.RegularColumn)

	tableId := uuid.New()
	cSpecs := []*cql_store.LocusDBColumnSpec{testColId, testColCluster, testCol1, testCol2}

	tableSpec := cql_store.NewLocusDBTableSpec(tableId, "sampleTable", cSpecs)
	return tableSpec
}

func TestCqlRequestHandler_insertToRow(t *testing.T) {
	var id1 = ids.GetIDFromString("1.1")
	cqlStore := setupStore(*id1)
	assert.NotNil(t, cqlStore)
	cqlStore.Initialize()
	sampleTblSpec := getSampleTableSpec()
	// create sample table in the store
	cmd := &store.Command{
		//TableUUID:           tableIdForPaxosTests,
		ConflictDomainKey: conflict_domain.DdlConflictDomain,
		CommandID:         ids.CommandID(1),
		OperationType:     store.DDL_WRITE,
		Operation:         *sampleTblSpec,
		TargetStore:       store.CQL,
	}
	_, err := cqlStore.ApplyCommand(cmd, false)
	assert.True(t, err == nil)
	rh := NewCqlRequestHandler(*id1, cfg, nil, cqlStore, cqlStore.GetConflictDomainResolver(conflict_domain.ConflictResolverCql))

	// now create an insert struct
	insert := cql_parser.InsertStatement{
		TableName: "sampleTable",
		Columns:   []string{"id", "colcluster", "col1", "col2"},
		Values:    []cql_data_model.LocusDBValue{cql_data_model.NewTextValue("testpk"), cql_data_model.NewTextValue("testck"), cql_data_model.NewTextValue("testval"), cql_data_model.NewInt(42)},
	}

	row, tblId, hasExpression, err := rh.insertToRow(insert)
	assert.True(t, err == nil)
	assert.False(t, hasExpression)
	assert.Equal(t, sampleTblSpec.GetTableUUID(), tblId)
	assert.Equal(t, "testpk", row[0].String())
	assert.Equal(t, "testck", row[1].String())
	assert.Equal(t, "testval", row[2].String())
	assert.Equal(t, cql_data_model.Int, row[3].GetType())
	assert.Equal(t, "42", row[3].String())
}

func TestCqlRequestHandler_updateToRow(t *testing.T) {
	var id1 = ids.GetIDFromString("1.1")
	cqlStore := setupStore(*id1)
	assert.NotNil(t, cqlStore)
	cqlStore.Initialize()
	sampleTblSpec := getSampleTableSpec()
	// create sample table in the store
	cmd := &store.Command{
		//TableUUID:           tableIdForPaxosTests,
		ConflictDomainKey: conflict_domain.DdlConflictDomain,
		CommandID:         ids.CommandID(1),
		OperationType:     store.DDL_WRITE,
		Operation:         *sampleTblSpec,
		TargetStore:       store.CQL,
	}
	_, err := cqlStore.ApplyCommand(cmd, false)
	assert.True(t, err == nil)
	rh := NewCqlRequestHandler(*id1, cfg, nil, cqlStore, cqlStore.GetConflictDomainResolver(conflict_domain.ConflictResolverCql))

	// now create an update struct
	update := cql_parser.UpdateStatement{
		TableName: "sampleTable",
		Assignments: []cql_parser.SimpleAssignment{
			{
				Column1Name: "col1",
				Value2:      cql_data_model.NewTextValue("testval"),
			},
			{
				Column1Name: "col2",
				Value2:      cql_data_model.NewInt(42),
			},
		},
		Relations: []*cql_parser.SimpleRelation{
			{
				Column1Name: "id",
				Const2:      cql_data_model.NewTextValue("testpk"),
				Relation:    cql_data_model.EQUAL,
			},
			{
				Column1Name: "colcluster",
				Const2:      cql_data_model.NewTextValue("testck"),
				Relation:    cql_data_model.EQUAL,
			},
		},
	}

	row, tblId, needsUpdateOp, err := rh.updateToRow(update)
	assert.True(t, err == nil)
	assert.False(t, needsUpdateOp)
	assert.Equal(t, sampleTblSpec.GetTableUUID(), tblId)
	assert.Equal(t, "testpk", row[0].String())
	assert.Equal(t, "testck", row[1].String())
	assert.Equal(t, "testval", row[2].String())
	assert.Equal(t, cql_data_model.Int, row[3].GetType())
	assert.Equal(t, "42", row[3].String())

	// create an update that covers an partition but has no expression
	update = cql_parser.UpdateStatement{
		TableName: "sampleTable",
		Assignments: []cql_parser.SimpleAssignment{
			{
				Column1Name: "col1",
				Value2:      cql_data_model.NewTextValue("testval"),
			},
			{
				Column1Name: "col2",
				Value2:      cql_data_model.NewInt(42),
			},
		},
		Relations: []*cql_parser.SimpleRelation{
			{
				Column1Name: "id",
				Const2:      cql_data_model.NewTextValue("testpk"),
				Relation:    cql_data_model.EQUAL,
			},
		},
	}

	row, tblId, needsUpdateOp, err = rh.updateToRow(update)
	assert.True(t, err == nil)
	assert.True(t, needsUpdateOp)
	assert.Equal(t, sampleTblSpec.GetTableUUID(), tblId)
	assert.Equal(t, "testpk", row[0].String())
	assert.Nil(t, row[1])
	assert.Equal(t, "testval", row[2].String())
	assert.Equal(t, cql_data_model.Int, row[3].GetType())
	assert.Equal(t, "42", row[3].String())

	// create an update that has an expression, hence requires a read
	update = cql_parser.UpdateStatement{
		TableName: "sampleTable",
		Assignments: []cql_parser.SimpleAssignment{
			{
				Column1Name: "col1",
				Value2:      cql_data_model.NewTextValue("testval"),
			},
			{
				Column1Name: "col2",
				Value2:      cql_data_model.NewExpressionValue("col2+1"),
			},
		},
		Relations: []*cql_parser.SimpleRelation{
			{
				Column1Name: "id",
				Const2:      cql_data_model.NewTextValue("testpk"),
				Relation:    cql_data_model.EQUAL,
			},
			{
				Column1Name: "colcluster",
				Const2:      cql_data_model.NewTextValue("testck"),
				Relation:    cql_data_model.EQUAL,
			},
		},
	}

	row, tblId, needsUpdateOp, err = rh.updateToRow(update)
	assert.True(t, err == nil)
	assert.True(t, needsUpdateOp)
	assert.Equal(t, sampleTblSpec.GetTableUUID(), tblId)
	assert.Equal(t, "testpk", row[0].String())
	assert.Equal(t, "testck", row[1].String())
	assert.Equal(t, "testval", row[2].String())
	assert.Equal(t, cql_data_model.Expression, row[3].GetType())
	assert.Equal(t, "col2+1", row[3].String())

	// create bad update struct
	update = cql_parser.UpdateStatement{
		TableName: "sampleTable",
		Assignments: []cql_parser.SimpleAssignment{
			{
				Column1Name: "col1",
				Value2:      cql_data_model.NewTextValue("testval"),
			},
			{
				Column1Name: "col2",
				Value2:      cql_data_model.NewInt(42),
			},
		},
		Relations: []*cql_parser.SimpleRelation{
			{
				Column1Name: "id",
				Const2:      cql_data_model.NewTextValue("testpk"),
				Relation:    cql_data_model.EQUAL,
			},
			{
				Column1Name: "colcluster",
				Const2:      cql_data_model.NewTextValue("testck"),
				Relation:    cql_data_model.GREATERTHAN,
			},
		},
	}

	row, tblId, needsUpdateOp, err = rh.updateToRow(update)
	assert.True(t, err != nil)
}

func TestCqlRequestHandler_selectToPkAndCks(t *testing.T) {
	var id1 = ids.GetIDFromString("1.1")
	cqlStore := setupStore(*id1)
	assert.NotNil(t, cqlStore)
	cqlStore.Initialize()
	sampleTblSpec := getSampleTableSpec()
	// create sample table in the store
	cmd := &store.Command{
		//TableUUID:           tableIdForPaxosTests,
		ConflictDomainKey: conflict_domain.DdlConflictDomain,
		CommandID:         ids.CommandID(1),
		OperationType:     store.DDL_WRITE,
		Operation:         *sampleTblSpec,
		TargetStore:       store.CQL,
	}
	_, err := cqlStore.ApplyCommand(cmd, false)
	assert.True(t, err == nil)
	rh := NewCqlRequestHandler(*id1, cfg, nil, cqlStore, cqlStore.GetConflictDomainResolver(conflict_domain.ConflictResolverCql))

	// create a select struct with wrong column
	selectQuery := cql_parser.SelectQuery{
		TableName:  "sampleTable",
		Columns:    []string{"id", "colcolumn", "col1"},
		AllColumns: false,
		Aliases:    map[int]string{1: "ck"},
		Relations:  nil,
	}

	rm, err := rh.selectToReadInfo(selectQuery)
	assert.True(t, err != nil)
	assert.True(t, rm == nil)

	// create a select struct with all good columns and no where
	selectQuery = cql_parser.SelectQuery{
		TableName:  "sampleTable",
		Columns:    []string{"id", "colcluster", "col1"},
		AllColumns: false,
		Aliases:    map[int]string{1: "ck"},
		Relations:  nil,
	}

	rm, err = rh.selectToReadInfo(selectQuery)
	assert.True(t, err != nil)
	log.Debugf("Error: %v", err)

	// create a select struct with all good columns explicitly listed and partition load
	selectQuery = cql_parser.SelectQuery{
		TableName:  "sampleTable",
		Columns:    []string{"id", "colcluster", "col1"},
		AllColumns: false,
		Aliases:    map[int]string{1: "ck"},
		Relations: []*cql_parser.SimpleRelation{
			{
				Column1Name: "id",
				Const2:      cql_data_model.NewTextValue("testkey"),
				Relation:    cql_data_model.EQUAL,
			},
		},
	}

	rm, err = rh.selectToReadInfo(selectQuery)
	assert.True(t, err == nil)
	if rm == nil {
		assert.Fail(t, "read metadata is nil")
		return
	}
	assert.Equal(t, uint8(0), rm.readop.ColIds[0])
	assert.Equal(t, uint8(1), rm.readop.ColIds[1])
	assert.Equal(t, uint8(2), rm.readop.ColIds[2])
	assert.Equal(t, "id", rm.namesOrAliases[0])
	assert.Equal(t, "ck", rm.namesOrAliases[1]) // make sure to use alias
	assert.Equal(t, "col1", rm.namesOrAliases[2])

	// create a select struct with all columns and partition load
	selectQuery = cql_parser.SelectQuery{
		TableName:  "sampleTable",
		AllColumns: true,
		Relations: []*cql_parser.SimpleRelation{
			{
				Column1Name: "id",
				Const2:      cql_data_model.NewTextValue("testkey"),
				Relation:    cql_data_model.EQUAL,
			},
		},
	}

	rm, err = rh.selectToReadInfo(selectQuery)
	assert.True(t, err == nil)
	if rm == nil {
		assert.Fail(t, "read metadata is nil")
		return
	}
	assert.Equal(t, uint8(0), rm.readop.ColIds[0])
	assert.Equal(t, uint8(1), rm.readop.ColIds[1])
	assert.Equal(t, uint8(2), rm.readop.ColIds[2])
	assert.Equal(t, "id", rm.namesOrAliases[0])
	assert.Equal(t, "colcluster", rm.namesOrAliases[1])
	assert.Equal(t, "col1", rm.namesOrAliases[2])

	// create a select struct with all good columns in different order
	selectQuery = cql_parser.SelectQuery{
		TableName:  "sampleTable",
		Columns:    []string{"id", "col2", "colcluster"},
		AllColumns: false,
		Aliases:    map[int]string{2: "ck"},
		Relations: []*cql_parser.SimpleRelation{
			{
				Column1Name: "id",
				Const2:      cql_data_model.NewTextValue("testkey"),
				Relation:    cql_data_model.EQUAL,
			},
		},
	}

	rm, err = rh.selectToReadInfo(selectQuery)
	assert.True(t, err == nil)
	if rm == nil {
		assert.Fail(t, "read metadata is nil")
		return
	}
	assert.Equal(t, uint8(0), rm.readop.ColIds[0]) // column with colid=1 is on 0th place
	assert.Equal(t, uint8(3), rm.readop.ColIds[1]) // column with colId=3 is on 1st place
	assert.Equal(t, uint8(1), rm.readop.ColIds[2]) // column with colid=1 is on 2nd place

	assert.Equal(t, "id", rm.namesOrAliases[0])
	assert.Equal(t, "col2", rm.namesOrAliases[1])
	assert.Equal(t, "ck", rm.namesOrAliases[2])        // make sure to use alias
	assert.Equal(t, readop.PREFIX, rm.readop.ReadMode) // partition read is always a prefix type

	// create a select struct with all good columns in different order
	selectQuery = cql_parser.SelectQuery{
		TableName:  "sampleTable",
		Columns:    []string{"id", "col2", "colcluster"},
		AllColumns: false,
		Aliases:    map[int]string{2: "ck"},
		Relations: []*cql_parser.SimpleRelation{
			{
				Column1Name: "id",
				Const2:      cql_data_model.NewTextValue("testkey"),
				Relation:    cql_data_model.EQUAL,
			},
			{
				Column1Name: "colcluster",
				Const2:      cql_data_model.NewTextValue("ck"),
				Relation:    cql_data_model.GREATERTHAN,
			},
		},
	}

	rm, err = rh.selectToReadInfo(selectQuery)
	assert.True(t, err == nil)
	if rm == nil {
		assert.Fail(t, "read metadata is nil")
		return
	}
	assert.Equal(t, uint8(0), rm.readop.ColIds[0]) // column with colid=1 is on 0th place
	assert.Equal(t, uint8(3), rm.readop.ColIds[1]) // column with colId=3 is on 1st place
	assert.Equal(t, uint8(1), rm.readop.ColIds[2]) // column with colid=1 is on 2nd place

	assert.Equal(t, "id", rm.namesOrAliases[0])
	assert.Equal(t, "col2", rm.namesOrAliases[1])
	assert.Equal(t, "ck", rm.namesOrAliases[2])       // make sure to use alias
	assert.Equal(t, readop.RANGE, rm.readop.ReadMode) // partition slice read is a range in this case
}

func BenchmarkCqlRequestHandler_selectToPkAndCks(b *testing.B) {
	var id1 = ids.GetIDFromString("1.1")
	cqlStore := setupStore(*id1)
	cqlStore.Initialize()
	sampleTblSpec := getSampleTableSpec()
	// create sample table in the store
	cmd := &store.Command{
		//TableUUID:           tableIdForPaxosTests,
		ConflictDomainKey: conflict_domain.DdlConflictDomain,
		CommandID:         ids.CommandID(1),
		OperationType:     store.DDL_WRITE,
		Operation:         *sampleTblSpec,
		TargetStore:       store.CQL,
	}
	cqlStore.ApplyCommand(cmd, false)
	rh := NewCqlRequestHandler(*id1, cfg, nil, cqlStore, cqlStore.GetConflictDomainResolver(conflict_domain.ConflictResolverCql))

	// create a select struct with all good columns in different order
	selectQuery := cql_parser.SelectQuery{
		TableName:  "sampleTable",
		Columns:    []string{"id", "col2", "colcluster"},
		AllColumns: false,
		Aliases:    map[int]string{2: "ck"},
		Relations: []*cql_parser.SimpleRelation{
			{
				Column1Name: "id",
				Const2:      cql_data_model.NewTextValue("testkey"),
				Relation:    cql_data_model.EQUAL,
			},
			{
				Column1Name: "colcluster",
				Const2:      cql_data_model.NewTextValue("ck"),
				Relation:    cql_data_model.GREATERTHAN,
			},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rh.selectToReadInfo(selectQuery)
	}
}

func TestCqlRequestHandler_compileReplicationOperations(t *testing.T) {
	var id1 = ids.GetIDFromString("1.1")
	cqlStore := setupStore(*id1)
	assert.NotNil(t, cqlStore)
	cqlStore.Initialize()
	sampleTblSpec := getSampleTableSpec()
	// create sample table in the store
	cmd := &store.Command{
		//TableUUID:           tableIdForPaxosTests,
		ConflictDomainKey: conflict_domain.DdlConflictDomain,
		CommandID:         ids.CommandID(1),
		OperationType:     store.DDL_WRITE,
		Operation:         *sampleTblSpec,
		TargetStore:       store.CQL,
	}
	_, err := cqlStore.ApplyCommand(cmd, false)
	assert.True(t, err == nil)
	cdResolver := cqlStore.GetConflictDomainResolver(conflict_domain.ConflictResolverCql)
	rh := NewCqlRequestHandler(*id1, cfg, nil, cqlStore, cdResolver)
	tbl := cqlStore.GetTable(sampleTblSpec.TableUUID)

	// now create a sample row
	row := &cql_data_model.Row{}
	row.SetCell(0, cql_data_model.NewTextValue("testkey"))
	row.SetCell(1, cql_data_model.NewTextValue("testcluster"))
	row.SetCell(2, cql_data_model.NewTextValue("testval"))
	row.SetCell(3, cql_data_model.NewInt(42))

	writeOps := make(map[string]map[int]writeop.WriteOp, 0)

	writeOp, cdKeyB64, err := tbl.RowToWriteOp(row, sampleTblSpec.GetTableUUID(), cdResolver)
	assert.True(t, err == nil)
	assert.NotNil(t, writeOp)
	writeOps[cdKeyB64] = make(map[int]writeop.WriteOp, 0)
	writeOps[cdKeyB64][0] = *writeOp

	readInfos := make(map[string]map[int]*readMeta, 0)
	cdCounts := make(map[string]int, 0)
	cdCounts[cdKeyB64] = 1

	req := &request.CqlRequest{
		ClientID:  *ids.NewClientID(1, 1),
		RequestId: 42,
		Timestamp: 0100,
	}

	rw := rh.compileReplicationOperations(writeOps, nil, nil, readInfos, cdCounts, req, nil)
	assert.Equal(t, 1, len(rw.replicationRequests))
	assert.Equal(t, store.WRITE, rw.replicationRequests[0].Command.OperationType)
	assert.Equal(t, *writeOp, rw.replicationRequests[0].Command.Operation.(writeop.WriteOp))

	row2 := &cql_data_model.Row{}
	row2.SetCell(0, cql_data_model.NewTextValue("testkey2"))
	row2.SetCell(1, cql_data_model.NewTextValue("testcluster2"))
	row2.SetCell(2, cql_data_model.NewTextValue("testval2"))
	row2.SetCell(3, cql_data_model.NewInt(43))

	writeOp, cdKeyB64, err = tbl.RowToWriteOp(row2, sampleTblSpec.GetTableUUID(), cdResolver)
	assert.True(t, err == nil)
	writeOps[cdKeyB64] = make(map[int]writeop.WriteOp, 0)
	writeOps[cdKeyB64][0] = *writeOp
	cdCounts[cdKeyB64] = 1

	req2 := &request.CqlRequest{
		ClientID:  *ids.NewClientID(1, 1),
		RequestId: 423,
		Timestamp: 0101,
	}

	rw2 := rh.compileReplicationOperations(writeOps, nil, nil, readInfos, cdCounts, req2, nil)
	assert.Equal(t, 2, len(rw2.replicationRequests))
	assert.Equal(t, store.WRITE, rw2.replicationRequests[0].Command.OperationType)
	assert.Equal(t, store.WRITE, rw2.replicationRequests[1].Command.OperationType)

	// create a select struct with all good columns in different order
	selectQuery := cql_parser.SelectQuery{
		TableName:  "sampleTable",
		Columns:    []string{"id", "col2", "colcluster"},
		AllColumns: false,
		Aliases:    map[int]string{2: "ck"},
		Relations: []*cql_parser.SimpleRelation{
			{
				Column1Name: "id",
				Const2:      cql_data_model.NewTextValue("testkey"),
				Relation:    cql_data_model.EQUAL,
			},
		},
	}

	rm, err := rh.selectToReadInfo(selectQuery)
	assert.True(t, err == nil)
	assert.True(t, rm != nil)

	readInfos[rm.ConflictDomainKey.B64()] = make(map[int]*readMeta)
	readInfos[rm.ConflictDomainKey.B64()][0] = rm
	req = &request.CqlRequest{
		ClientID:  *ids.NewClientID(1, 1),
		RequestId: 42,
		Timestamp: 0100,
	}
	writeOpsBlank := make(map[string]map[int]writeop.WriteOp, 0)
	cdCounts = make(map[string]int, 0)
	cdCounts[rm.ConflictDomainKey.B64()] = 1

	rw = rh.compileReplicationOperations(writeOpsBlank, nil, nil, readInfos, cdCounts, req, nil)
	assert.Equal(t, 1, len(rw.replicationRequests))
	assert.Equal(t, store.READ, rw.replicationRequests[0].Command.OperationType)
	readOp := rw.replicationRequests[0].Command.Operation.(readop.CQLReadOp)
	assert.Equal(t, *rm.readop, readOp)
}
