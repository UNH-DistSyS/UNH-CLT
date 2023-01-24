package cql_store

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/UNH-DistSyS/UNH-CLT/conflict_domain"
	"github.com/UNH-DistSyS/UNH-CLT/log"
	"github.com/UNH-DistSyS/UNH-CLT/store"
	"github.com/UNH-DistSyS/UNH-CLT/store/cql_store/cql_data_model"
	"github.com/UNH-DistSyS/UNH-CLT/utils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func getOnePKNoClusteringAllIntTableSpecs() *LocusDBTableSpec {
	pkspec := &LocusDBColumnSpec{
		Name:     "pk",
		DataType: cql_data_model.Int,
		Id:       0,
		Kind:     PartitionColumn,
	}

	rc := &LocusDBColumnSpec{
		Name:     "c1",
		DataType: cql_data_model.Int,
		Id:       1,
		Kind:     RegularColumn,
	}

	tableId := uuid.New()
	cSpecs := []*LocusDBColumnSpec{pkspec, rc}

	tableSpec := NewLocusDBTableSpec(tableId, "testTbl", cSpecs)
	return tableSpec
}

func getOnePKTextNoClusteringAllIntTableSpecs() *LocusDBTableSpec {
	pkspec := &LocusDBColumnSpec{
		Name:     "pk",
		DataType: cql_data_model.Text,
		Id:       0,
		Kind:     PartitionColumn,
	}

	rc := &LocusDBColumnSpec{
		Name:     "c1",
		DataType: cql_data_model.Boolean,
		Id:       1,
		Kind:     RegularColumn,
	}

	tableId := uuid.New()
	cSpecs := []*LocusDBColumnSpec{pkspec, rc}

	tableSpec := NewLocusDBTableSpec(tableId, "testTbl", cSpecs)
	return tableSpec
}

func getOnePKAllIntTableSpecs() *LocusDBTableSpec {
	pkspec := &LocusDBColumnSpec{
		Name:     "pk",
		DataType: cql_data_model.Int,
		Id:       0,
		Kind:     PartitionColumn,
	}
	ckspec := &LocusDBColumnSpec{
		Name:     "ck",
		DataType: cql_data_model.Int,
		Id:       1,
		Kind:     ClusteringColumn,
	}

	rc := &LocusDBColumnSpec{
		Name:     "c1",
		DataType: cql_data_model.Int,
		Id:       2,
		Kind:     RegularColumn,
	}

	tableId := uuid.New()
	cSpecs := []*LocusDBColumnSpec{pkspec, ckspec, rc}

	tableSpec := NewLocusDBTableSpec(tableId, "testTbl", cSpecs)
	return tableSpec
}

func getOnePKThreeColsTableSpecs() *LocusDBTableSpec {
	pkspec := &LocusDBColumnSpec{
		Name:     "pk",
		DataType: cql_data_model.Int,
		Id:       0,
		Kind:     PartitionColumn,
	}
	ckspec := &LocusDBColumnSpec{
		Name:     "ck",
		DataType: cql_data_model.Int,
		Id:       1,
		Kind:     ClusteringColumn,
	}

	rc := &LocusDBColumnSpec{
		Name:     "c1",
		DataType: cql_data_model.Int,
		Id:       2,
		Kind:     RegularColumn,
	}

	rc2 := &LocusDBColumnSpec{
		Name:     "c2",
		DataType: cql_data_model.Float,
		Id:       3,
		Kind:     RegularColumn,
	}

	rc3 := &LocusDBColumnSpec{
		Name:     "c3",
		DataType: cql_data_model.Text,
		Id:       4,
		Kind:     RegularColumn,
	}

	tableId := uuid.New()
	cSpecs := []*LocusDBColumnSpec{pkspec, ckspec, rc, rc2, rc3}

	tableSpec := NewLocusDBTableSpec(tableId, "testTbl", cSpecs)
	return tableSpec
}

func TestNewLocusDBTableSpec(t *testing.T) {
	tableSpec := getOnePKAllIntTableSpecs()
	assert.Equal(t, 1, tableSpec.pkCount)
	assert.Equal(t, 1, tableSpec.ckCount)
	assert.Equal(t, RegularColumn, tableSpec.GetColumnSpecByName("c1").Kind)
	assert.Equal(t, cql_data_model.Int, tableSpec.GetColumnSpecByName("c1").DataType)
	assert.Equal(t, PartitionColumn, tableSpec.GetColumnSpecByName("pk").Kind)
	assert.Equal(t, cql_data_model.Int, tableSpec.GetColumnSpecByName("pk").DataType)
	assert.Equal(t, ClusteringColumn, tableSpec.GetColumnSpecByName("ck").Kind)
	assert.Equal(t, cql_data_model.Int, tableSpec.GetColumnSpecByName("ck").DataType)

	tableSpec = getOnePKNoClusteringAllIntTableSpecs()
	assert.Equal(t, 1, tableSpec.pkCount)
	assert.Equal(t, 0, tableSpec.ckCount)
	assert.Equal(t, RegularColumn, tableSpec.GetColumnSpecByName("c1").Kind)
	assert.Equal(t, cql_data_model.Int, tableSpec.GetColumnSpecByName("c1").DataType)
	assert.Equal(t, PartitionColumn, tableSpec.GetColumnSpecByName("pk").Kind)
	assert.Equal(t, cql_data_model.Int, tableSpec.GetColumnSpecByName("pk").DataType)
}

func TestLocusDBTableTranslator_RowToKV_OnePKNoClusteringAllInt(t *testing.T) {
	tableSpec := getOnePKNoClusteringAllIntTableSpecs()
	translator := CQLTable{tableSpec: tableSpec}
	badRow := cql_data_model.Row{1: cql_data_model.NewInt(42)}

	kv, _, err := translator.RowToKVItems(badRow)
	if err == nil {
		assert.Fail(t, "Expecting error, got none")
		assert.Equal(t, ErrNotEnoughKeyComponents, err.Error())
	}

	goodRow1 := cql_data_model.Row{0: cql_data_model.NewInt(12), 1: cql_data_model.NewInt(260)}

	kv, _, err = translator.RowToKVItems(goodRow1)

	log.Debugf("kv : %v", kv)
	expectedVal := []byte{0, 0, 1, 4}
	assert.True(t, utils.AreByteStringsSame(expectedVal, kv[0].Value))
}

func TestTestLocusDBTableTranslator_RowToKV_OnePKTextNoClusteringAllInt(t *testing.T) {
	tableSpec := getOnePKTextNoClusteringAllIntTableSpecs()
	translator := CQLTable{tableSpec: tableSpec}
	badRow := cql_data_model.Row{1: cql_data_model.NewBooleanValue(true)}

	kv, _, err := translator.RowToKVItems(badRow)
	if err == nil {
		assert.Fail(t, "Expecting error, got none")
		assert.Equal(t, ErrNotEnoughKeyComponents, err.Error())
	}

	goodRow1 := cql_data_model.Row{0: cql_data_model.NewTextValue("testkey"), 1: cql_data_model.NewBooleanValue(true)}

	kv, _, err = translator.RowToKVItems(goodRow1)

	log.Debugf("kv : %v", kv)
	expectedVal := []byte{1}
	expectedKey := []byte{1, 5, 0, 0, 0, 7, 116, 101, 115, 116, 107, 101, 121, 3, 1}
	assert.True(t, utils.AreByteStringsSame(expectedVal, kv[0].Value))
	assert.True(t, utils.AreByteStringsSame(expectedKey, kv[0].Key))
}

func TestTestLocusDBTableTranslator_RowToKV_OnePKOneClusteringAllInt(t *testing.T) {
	tableSpec := getOnePKAllIntTableSpecs()
	translator := CQLTable{tableSpec: tableSpec}
	badRow := cql_data_model.Row{1: cql_data_model.NewInt(42)}

	kv, _, err := translator.RowToKVItems(badRow)
	if err == nil {
		assert.Fail(t, "Expecting error, got none")
		assert.Equal(t, ErrNotEnoughKeyComponents, err.Error())
	}
	goodRow1 := cql_data_model.Row{0: cql_data_model.NewInt(12), 1: cql_data_model.NewInt(42), 2: cql_data_model.NewInt(260)}

	kv, _, err = translator.RowToKVItems(goodRow1)

	log.Debugf("kv : %v", kv)
	expectedVal := []byte{0, 0, 1, 4}
	assert.True(t, utils.AreByteStringsSame(expectedVal, kv[0].Value))

	expectedKey := []byte{1, 1, 0, 0, 0, 12, 2, 1, 0, 0, 0, 42, 3, 2}
	assert.True(t, utils.AreByteStringsSame(expectedKey, kv[0].Key))
}

func TestTestLocusDBTableTranslator_RowToKV_OnePKOneClusteringThreeRegularCols(t *testing.T) {
	tableSpec := getOnePKThreeColsTableSpecs()
	translator := CQLTable{tableSpec: tableSpec}
	goodRow1 := cql_data_model.Row{0: cql_data_model.NewInt(12), 1: cql_data_model.NewInt(42), 2: cql_data_model.NewInt(260), 3: cql_data_model.NewFloat(42.42), 4: cql_data_model.NewTextValue("hi there!")}

	kv, _, err := translator.RowToKVItems(goodRow1)
	if err != nil {
		assert.Fail(t, fmt.Sprintf("unexpected error: %v", err))
	}

	log.Debugf("kv : %v", kv)
	assert.Equal(t, 3, len(kv))

	expectedVal1 := []byte{0, 0, 1, 4}
	expectedVal2 := []byte{66, 41, 174, 20}
	expectedVal3 := []byte{0, 0, 0, 9, 104, 105, 32, 116, 104, 101, 114, 101, 33}
	expectedVals := [][]byte{expectedVal1, expectedVal2, expectedVal3}

	expectedKey1 := []byte{1, 1, 0, 0, 0, 12, 2, 1, 0, 0, 0, 42, 3, 2}
	expectedKey2 := []byte{1, 1, 0, 0, 0, 12, 2, 1, 0, 0, 0, 42, 3, 3}
	expectedKey3 := []byte{1, 1, 0, 0, 0, 12, 2, 1, 0, 0, 0, 42, 3, 4}
	expectedKeys := [][]byte{expectedKey1, expectedKey2, expectedKey3}

	// check if we have all expected values once
	for _, item := range kv {
		expectedOk := false
		for j, v := range expectedVals {
			if v != nil && utils.AreByteStringsSame(v, item.Value) {
				expectedVals[j] = nil
				expectedOk = true
			}
		}
		assert.True(t, expectedOk)
	}
	// check if we have all expected keys once
	for _, item := range kv {
		expectedOk := false
		for j, k := range expectedKeys {
			if k != nil && utils.AreByteStringsSame(k, item.Key) {
				expectedKeys[j] = nil
				expectedOk = true
			}
		}
		assert.True(t, expectedOk)
	}
}

func TestCqlRequestHandler_rowToWriteOp(t *testing.T) {
	tableSpec := getOnePKThreeColsTableSpecs()
	translator := CQLTable{tableSpec: tableSpec}
	cdResolver := conflict_domain.NewCQLConflictDomainResolver()
	// create a sample row
	row := &cql_data_model.Row{}
	row.SetCell(0, cql_data_model.NewTextValue("testkey"))
	row.SetCell(1, cql_data_model.NewTextValue("testcluster"))
	row.SetCell(2, cql_data_model.NewTextValue("testval"))
	row.SetCell(3, cql_data_model.NewInt(42))

	writeOp, pk, err := translator.RowToWriteOp(row, tableSpec.GetTableUUID(), cdResolver)
	assert.True(t, err == nil)
	assert.NotNil(t, writeOp)
	assert.True(t, len(pk) > 0)

	row2 := &cql_data_model.Row{}
	row2.SetCell(0, cql_data_model.NewTextValue("testkey2"))
	row2.SetCell(1, cql_data_model.NewTextValue("testcluster2"))
	row2.SetCell(2, cql_data_model.NewTextValue("testval2"))
	row2.SetCell(3, cql_data_model.NewInt(43))

	writeOp, pk2, err := translator.RowToWriteOp(row2, tableSpec.GetTableUUID(), cdResolver)
	assert.True(t, err == nil)
	assert.NotNil(t, writeOp)
	assert.True(t, len(pk2) > 0)
	assert.NotEqual(t, pk, pk2)
}

func BenchmarkLocusDBTableTranslator_RowToKV_OnePKOneClusteringThreeRegularCols(b *testing.B) {
	tableSpec := getOnePKThreeColsTableSpecs()
	translator := CQLTable{tableSpec: tableSpec}
	goodRow1 := cql_data_model.Row{0: cql_data_model.NewInt(12), 1: cql_data_model.NewInt(42), 2: cql_data_model.NewInt(260), 3: cql_data_model.NewFloat(42.42), 4: cql_data_model.NewTextValue("hi there!")}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := translator.RowToKVItems(goodRow1)
		if err != nil {
			log.Debugf("Unexpected Error: %v", err)
		}
	}
}

func BenchmarkLocusDBTableTranslator_RowsToKV_OnePKOneClusteringThreeRegularCols(b *testing.B) {
	tableSpec := getOnePKThreeColsTableSpecs()
	translator := CQLTable{tableSpec: tableSpec}
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	// do 100 Rows
	rows := make([]cql_data_model.Row, 0, 100)
	for i := 0; i < 100; i++ {
		str := utils.RandString(1024, rnd) // 1 kb random string
		row := cql_data_model.Row{0: cql_data_model.NewInt(int32(i) + 12), 1: cql_data_model.NewInt(int32(i) + 42), 2: cql_data_model.NewInt(260 * int32(i)), 3: cql_data_model.NewFloat(42.42 + float32(i)), 4: cql_data_model.NewTextValue(str)}
		rows = append(rows, row)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := translator.RowsToKVItems(rows)
		if err != nil {
			log.Debugf("Unexpected Error: %v", err)
		}
	}
}

func TestLocusDBTableTranslator_TranslateKVReadResultsToRows(t *testing.T) {
	tableSpec := getOnePKThreeColsTableSpecs()
	translator := CQLTable{tableSpec: tableSpec}

	expectedVal1 := []byte{0, 0, 1, 4}
	expectedVal2 := []byte{66, 41, 174, 20}
	expectedVal3 := []byte{0, 0, 0, 9, 104, 105, 32, 116, 104, 101, 114, 101, 33}

	expectedKey1 := []byte{1, 1, 0, 0, 0, 12, 2, 1, 0, 0, 0, 42, 3, 2}
	expectedKey2 := []byte{1, 1, 0, 0, 0, 12, 2, 1, 0, 0, 0, 42, 3, 3}
	expectedKey3 := []byte{1, 1, 0, 0, 0, 12, 2, 1, 0, 0, 0, 42, 3, 4}

	kvItems := []store.KVItem{
		{Key: expectedKey1, Value: expectedVal1},
		{Key: expectedKey2, Value: expectedVal2},
		{Key: expectedKey3, Value: expectedVal3},
	}

	rc, err := translator.TranslateKVReadResultsToRows(kvItems, nil, nil)
	if err != nil {
		assert.Fail(t, fmt.Sprintf("unexpected error: %v", err))
	}

	assert.Equal(t, 1, rc.Size())
	assert.Equal(t, int32(12), rc.Rows[0][0].(*cql_data_model.IntValue).Val)
	assert.Equal(t, int32(42), rc.Rows[0][1].(*cql_data_model.IntValue).Val)
	assert.Equal(t, int32(260), rc.Rows[0][2].(*cql_data_model.IntValue).Val)
	assert.Equal(t, float32(42.42), rc.Rows[0][3].(*cql_data_model.FloatValue).Val)
	assert.Equal(t, "hi there!", rc.Rows[0][4].(*cql_data_model.TextValue).Val)
}

func TestLocusDBTableTranslator_TranslateKVReadResultsToRowsWithColumnFilter(t *testing.T) {
	tableSpec := getOnePKThreeColsTableSpecs()
	translator := CQLTable{tableSpec: tableSpec}

	expectedVal1 := []byte{0, 0, 1, 4}
	expectedVal2 := []byte{66, 41, 174, 20}
	expectedVal3 := []byte{0, 0, 0, 9, 104, 105, 32, 116, 104, 101, 114, 101, 33}

	expectedKey1 := []byte{1, 1, 0, 0, 0, 12, 2, 1, 0, 0, 0, 42, 3, 2}
	expectedKey2 := []byte{1, 1, 0, 0, 0, 12, 2, 1, 0, 0, 0, 42, 3, 3}
	expectedKey3 := []byte{1, 1, 0, 0, 0, 12, 2, 1, 0, 0, 0, 42, 3, 4}

	kvItems := []store.KVItem{
		{Key: expectedKey1, Value: expectedVal1},
		{Key: expectedKey2, Value: expectedVal2},
		{Key: expectedKey3, Value: expectedVal3},
	}

	colFilter := make(map[uint8]struct{})
	colFilter[tableSpec.columnNameToId["ck"]] = struct{}{}
	colFilter[tableSpec.columnNameToId["c2"]] = struct{}{}

	rc, err := translator.TranslateKVReadResultsToRows(kvItems, colFilter, nil)
	if err != nil {
		assert.Fail(t, fmt.Sprintf("unexpected error: %v", err))
	}

	assert.Equal(t, 1, rc.Size())
	assert.Equal(t, nil, rc.Rows[0][0])
	assert.Equal(t, int32(42), rc.Rows[0][1].(*cql_data_model.IntValue).Val)
	assert.Equal(t, nil, rc.Rows[0][2])
	assert.Equal(t, float32(42.42), rc.Rows[0][3].(*cql_data_model.FloatValue).Val)
	assert.Equal(t, nil, rc.Rows[0][4])
}

func TestLocusDBTableTranslator_TranslateKVReadResultsToRows_MultipleRows(t *testing.T) {
	tableSpec := getOnePKThreeColsTableSpecs()
	translator := CQLTable{tableSpec: tableSpec}

	expectedVal1 := []byte{0, 0, 1, 4}
	expectedVal2 := []byte{66, 41, 174, 20}
	expectedVal3 := []byte{0, 0, 0, 9, 104, 105, 32, 116, 104, 101, 114, 101, 33}

	expectedVal4 := []byte{0, 0, 1, 5}
	expectedVal5 := []byte{66, 41, 174, 20}
	expectedVal6 := []byte{0, 0, 0, 8, 104, 105, 32, 116, 104, 101, 114, 101}

	expectedKey1 := []byte{1, 1, 0, 0, 0, 12, 2, 1, 0, 0, 0, 42, 3, 2}
	expectedKey2 := []byte{1, 1, 0, 0, 0, 12, 2, 1, 0, 0, 0, 42, 3, 3}
	expectedKey3 := []byte{1, 1, 0, 0, 0, 12, 2, 1, 0, 0, 0, 42, 3, 4}

	expectedKey4 := []byte{1, 1, 0, 0, 0, 12, 2, 1, 0, 0, 0, 43, 3, 2}
	expectedKey5 := []byte{1, 1, 0, 0, 0, 12, 2, 1, 0, 0, 0, 43, 3, 3}
	expectedKey6 := []byte{1, 1, 0, 0, 0, 12, 2, 1, 0, 0, 0, 43, 3, 4}

	kvItems := []store.KVItem{
		{Key: expectedKey1, Value: expectedVal1},
		{Key: expectedKey2, Value: expectedVal2},
		{Key: expectedKey3, Value: expectedVal3},
		{Key: expectedKey4, Value: expectedVal4},
		{Key: expectedKey5, Value: expectedVal5},
		{Key: expectedKey6, Value: expectedVal6},
	}

	rc, err := translator.TranslateKVReadResultsToRows(kvItems, nil, nil)
	if err != nil {
		assert.Fail(t, fmt.Sprintf("unexpected error: %v", err))
	}

	assert.Equal(t, 2, rc.Size())
	assert.Equal(t, int32(12), rc.Rows[0][0].(*cql_data_model.IntValue).Val)
	assert.Equal(t, int32(42), rc.Rows[0][1].(*cql_data_model.IntValue).Val)
	assert.Equal(t, int32(260), rc.Rows[0][2].(*cql_data_model.IntValue).Val)
	assert.Equal(t, float32(42.42), rc.Rows[0][3].(*cql_data_model.FloatValue).Val)
	assert.Equal(t, "hi there!", rc.Rows[0][4].(*cql_data_model.TextValue).Val)

	assert.Equal(t, int32(12), rc.Rows[1][0].(*cql_data_model.IntValue).Val)
	assert.Equal(t, int32(43), rc.Rows[1][1].(*cql_data_model.IntValue).Val)
	assert.Equal(t, int32(261), rc.Rows[1][2].(*cql_data_model.IntValue).Val)
	assert.Equal(t, float32(42.42), rc.Rows[1][3].(*cql_data_model.FloatValue).Val)
	assert.Equal(t, "hi there", rc.Rows[1][4].(*cql_data_model.TextValue).Val)
}

func TestLocusDBTableTranslator_TranslateKVReadResultsToRows_TextPK(t *testing.T) {
	tableSpec := getOnePKTextNoClusteringAllIntTableSpecs()
	translator := CQLTable{tableSpec: tableSpec}

	expectedVal := []byte{1}
	expectedKey := []byte{1, 5, 0, 0, 0, 7, 116, 101, 115, 116, 107, 101, 121, 3, 1}

	kvItems := []store.KVItem{
		{Key: expectedKey, Value: expectedVal},
	}

	rc, err := translator.TranslateKVReadResultsToRows(kvItems, nil, nil)
	if err != nil {
		assert.Fail(t, fmt.Sprintf("unexpected error: %v", err))
	}

	assert.Equal(t, 1, rc.Size())
	assert.Equal(t, "testkey", rc.Rows[0][0].(*cql_data_model.TextValue).Val)
	assert.Equal(t, true, rc.Rows[0][1].(*cql_data_model.BooleanValue).Val)
}

func TestLocusDBTableTranslator_ReadPartition_TextPK(t *testing.T) {
	tableSpec := getOnePKTextNoClusteringAllIntTableSpecs()
	translator := CQLTable{tableSpec: tableSpec}

	vals := []cql_data_model.LocusDBValue{cql_data_model.NewTextValue("testkey")}
	expectedKey := []byte{1, 5, 0, 0, 0, 7, 116, 101, 115, 116, 107, 101, 121}

	readOps := translator.ReadPartition(vals)

	assert.True(t, utils.AreByteStringsSame(expectedKey, readOps.StartKey))
}

func BenchmarkLocusDBTableTranslator_TranslateKVReadResultsToRows(b *testing.B) {
	tableSpec := getOnePKThreeColsTableSpecs()
	translator := CQLTable{tableSpec: tableSpec}

	expectedVal1 := []byte{0, 0, 1, 4}
	expectedVal2 := []byte{66, 41, 174, 20}
	expectedVal3 := []byte{0, 0, 0, 9, 104, 105, 32, 116, 104, 101, 114, 101, 33}

	expectedKey1 := []byte{1, 1, 0, 0, 0, 12, 2, 1, 0, 0, 0, 42, 3, 2}
	expectedKey2 := []byte{1, 1, 0, 0, 0, 12, 2, 1, 0, 0, 0, 42, 3, 3}
	expectedKey3 := []byte{1, 1, 0, 0, 0, 12, 2, 1, 0, 0, 0, 42, 3, 4}

	kvItems := []store.KVItem{
		{Key: expectedKey1, Value: expectedVal1},
		{Key: expectedKey2, Value: expectedVal2},
		{Key: expectedKey3, Value: expectedVal3},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		translator.TranslateKVReadResultsToRows(kvItems, nil, nil)
	}
}

func BenchmarkLocusDBTableTranslator_ReadSuspectedKey(b *testing.B) {
	tableSpec := getOnePKThreeColsTableSpecs()
	translator := CQLTable{tableSpec: tableSpec}
	key := []byte{1, 1, 0, 0, 0, 12, 2, 1, 0, 0, 0, 42, 3, 2}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		keyReader := bytes.NewReader(key)
		translator.ReadSuspectedKey(12, keyReader)
	}
}

func BenchmarkLocusDBTableTranslator_readKeyColumnsFromRawItem(b *testing.B) {
	tableSpec := getOnePKThreeColsTableSpecs()
	translator := CQLTable{tableSpec: tableSpec}
	key := []byte{1, 1, 0, 0, 0, 12, 2, 1, 0, 0, 0, 42, 3, 2}
	val := []byte{0, 0, 0, 9, 104, 105, 32, 116, 104, 101, 114, 101, 33}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		keyReader := bytes.NewReader(key)
		valueReader := bytes.NewReader(val)
		translator.readKeyColumnsFromRawItem(keyReader, valueReader, nil)
	}
}
