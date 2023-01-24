package cql_store

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/UNH-DistSyS/UNH-CLT/conflict_domain"
	"github.com/UNH-DistSyS/UNH-CLT/log"
	"github.com/UNH-DistSyS/UNH-CLT/replication/operations/readop"
	"github.com/UNH-DistSyS/UNH-CLT/replication/operations/writeop"
	"github.com/UNH-DistSyS/UNH-CLT/store"
	"github.com/UNH-DistSyS/UNH-CLT/store/cql_store/cql_data_model"
	"github.com/UNH-DistSyS/UNH-CLT/utils"
	"github.com/google/uuid"
)

func locusDBValueToKVKey(keyType uint8, key cql_data_model.LocusDBValue, keyPrefix []byte) []byte {
	searchKey := make([]byte, 0)
	searchKey = append(searchKey, keyType)
	coltype := key.GetType()
	searchKey = append(searchKey, coltype.GetVal())
	searchKey = append(searchKey, key.Serialize()...)
	keyPrefix = append(keyPrefix, searchKey...)
	return keyPrefix
}

type LocusDBColumnKind uint8

const (
	PartitionColumn LocusDBColumnKind = iota + 1
	ClusteringColumn
	RegularColumn
	NoCol // used to designed impossible column type, such that it is higher than any other type when serialized
)

func (ct LocusDBColumnKind) ToUint8() uint8 {
	return uint8(ct)
}

var (
	ErrInvalidDataType             = errors.New("invalid locusdb data type")
	ErrNotEnoughKeyComponents      = errors.New("table requires more partition or clustering key components")
	ErrPartitionOrClusteringIsNull = errors.New("partition or clustering key cannot be NULL")
)

func NewLocusDBColumnSpec(name string, dataType cql_data_model.LocusDBType, colId, orderNum uint8, colKind LocusDBColumnKind) *LocusDBColumnSpec {
	return &LocusDBColumnSpec{
		Name:     name,
		DataType: dataType,
		Id:       colId,
		OrderNum: orderNum,
		Kind:     colKind,
	}
}

type LocusDBColumnSpec struct {
	Name     string
	DataType cql_data_model.LocusDBType
	Id       uint8 // column Id. used in storage
	OrderNum uint8 // column order. important for partition and clustering keys, as these need to be order for search
	Kind     LocusDBColumnKind
}

func (spec *LocusDBColumnSpec) GetName() string {
	return spec.Name
}

func (spec *LocusDBColumnSpec) GetID() uint8 {
	return spec.Id
}

func (spec *LocusDBColumnSpec) GetOrderNum() uint8 {
	return spec.OrderNum
}

func (spec *LocusDBColumnSpec) GetKind() LocusDBColumnKind {
	return spec.Kind
}

func (spec *LocusDBColumnSpec) GetDataType() cql_data_model.LocusDBType {
	return spec.DataType
}

type LocusDBTableSpec struct {
	ColumnSpecs        map[uint8]*LocusDBColumnSpec // id -> column spec
	TableUUID          uuid.UUID
	TableName          string
	SkipCreateIfExists bool // we use this when table spec is used in DDL to communicate that it is ok to skip if table is already there
	columnNameToId     map[string]uint8
	pkCount            int // precomputed pk count value
	ckCount            int // precomputed clustering key count
}

func NewLocusDBTableSpec(tableId uuid.UUID, tableName string, columnSpecs []*LocusDBColumnSpec) *LocusDBTableSpec {
	columnSpecsMap := make(map[uint8]*LocusDBColumnSpec, len(columnSpecs))
	for _, colspec := range columnSpecs {
		columnSpecsMap[colspec.Id] = colspec
	}

	spec := &LocusDBTableSpec{
		ColumnSpecs: columnSpecsMap,
		TableUUID:   tableId,
		TableName:   tableName,
		pkCount:     0,
		ckCount:     0,
	}

	spec.InitTableSpec()

	return spec
}

func (spec *LocusDBTableSpec) InitTableSpec() {
	// init should be idempotent, so reset all variables we compute here
	spec.ckCount = 0
	spec.pkCount = 0
	spec.columnNameToId = make(map[string]uint8)
	for i, colSpec := range spec.ColumnSpecs {
		if colSpec.Kind == ClusteringColumn {
			spec.ckCount++
		}

		if colSpec.Kind == PartitionColumn {
			spec.pkCount++
		}

		spec.columnNameToId[colSpec.Name] = i
	}
}

func (spec *LocusDBTableSpec) GetName() string {
	return spec.TableName
}

func (spec *LocusDBTableSpec) GetPartitionKeyCount() int {
	return spec.pkCount
}

func (spec *LocusDBTableSpec) GetClusteringKeyCount() int {
	return spec.ckCount
}

func (spec *LocusDBTableSpec) GetColumnSpecByName(colName string) *LocusDBColumnSpec {
	if id, exists := spec.columnNameToId[colName]; exists {
		return spec.ColumnSpecs[id]
	}
	return nil
}

func (spec *LocusDBTableSpec) GetColumnSpecs() map[uint8]*LocusDBColumnSpec {
	return spec.ColumnSpecs
}

func (spec *LocusDBTableSpec) GetTableUUID() uuid.UUID {
	return spec.TableUUID
}

type CQLTable struct {
	tableSpec *LocusDBTableSpec
}

func (tbl *CQLTable) GetSchema() *LocusDBTableSpec {
	return tbl.tableSpec
}

/*************************************************************
* Table Write Helpers
* Here we translate Rows we got from the user into KVItems
* that can replicate and persist in underlying KV Store
**************************************************************/

func (tbl *CQLTable) PartitionKeyFromValues(pkvalues []cql_data_model.LocusDBValue) store.ByteString {
	pKey := make([]byte, 0, 16)
	for i, pk := range pkvalues {
		pKey = append(pKey, PartitionColumn.ToUint8())
		colSpec := tbl.tableSpec.ColumnSpecs[uint8(i)]
		pKey = append(pKey, colSpec.DataType.GetVal())
		pKey = append(pKey, pk.Serialize()...)
	}
	return pKey
}

func (tbl *CQLTable) RowsToKVItems(rows []cql_data_model.Row) ([]store.KVItem, error) {
	if len(rows) == 0 {
		log.Errorf("Error translating row to kv: no Rows")
		return nil, errors.New("error translating Rows to kv: no Rows")
	}

	kvItems := make([]store.KVItem, 0, len(rows)*len(rows[0]))
	for _, row := range rows {
		kv, _, err := tbl.RowToKVItems(row)
		if err != nil {
			log.Errorf("Error translating row to kv: %v", err)
			return nil, err
		}
		kvItems = append(kvItems, kv...)
	}
	return kvItems, nil
}

func (tbl *CQLTable) IsRowValid(row cql_data_model.Row) bool {
	if row == nil {
		return false
	}

	tableSpec := tbl.tableSpec

	numPkAndCk := 0
	for colId, val := range row {
		switch tableSpec.ColumnSpecs[colId].Kind {
		case PartitionColumn:
			if val.GetType() != cql_data_model.Null {
				numPkAndCk++
			}
		case ClusteringColumn:
			if val.GetType() != cql_data_model.Null {
				numPkAndCk++
			}
		}
		if numPkAndCk == tableSpec.pkCount+tableSpec.ckCount {
			break
		}
	}

	return tableSpec.pkCount+tableSpec.ckCount == numPkAndCk
}

func (tbl *CQLTable) RowToKVItems(row cql_data_model.Row) ([]store.KVItem, store.ByteString, error) {
	tableSpec := tbl.tableSpec

	pkvalues := make([]cql_data_model.LocusDBValue, tableSpec.pkCount)
	ckvalues := make([]cql_data_model.LocusDBValue, tableSpec.ckCount)
	numPkAndCk := 0
	for colId, val := range row {
		/*if val != nil && val.GetType() == Null {
			return nil, nil, ErrNotEnoughKeyComponents
		}*/
		switch tableSpec.ColumnSpecs[colId].Kind {
		case PartitionColumn:
			if val.GetType() != cql_data_model.Null {
				pkvalues[colId] = val
				numPkAndCk++
			}
		case ClusteringColumn:
			if val.GetType() != cql_data_model.Null {
				ckvalues[colId-uint8(tableSpec.pkCount)] = val
				numPkAndCk++
			}
		}
		if numPkAndCk == tableSpec.pkCount+tableSpec.ckCount {
			break
		}
	}

	if tableSpec.pkCount+tableSpec.ckCount != numPkAndCk {
		return nil, nil, ErrNotEnoughKeyComponents
	}

	cKey := make([]byte, 0, 16)
	pKey := tbl.PartitionKeyFromValues(pkvalues)

	for i, ck := range ckvalues {
		cKey = append(cKey, ClusteringColumn.ToUint8())
		colSpec := tableSpec.ColumnSpecs[uint8(i+tableSpec.pkCount)]
		cKey = append(cKey, colSpec.DataType.GetVal())
		cKey = append(cKey, ck.Serialize()...)
	}

	kvItems := make([]store.KVItem, 0, len(row))
	rowPrefix := pKey

	if tableSpec.ckCount > 0 {
		rowPrefix = append(rowPrefix, cKey...)
	}

	if len(row) == numPkAndCk {
		// row is only specified with PK and CK
		var val []byte
		kvItems = append(kvItems, store.KVItem{Key: rowPrefix, Value: val})
	} else {
		for colId, val := range row {
			colSpec := tableSpec.ColumnSpecs[colId]
			if colSpec.Kind == RegularColumn {
				key := make([]byte, len(rowPrefix)+2)
				copy(key, rowPrefix)
				key[len(key)-2] = RegularColumn.ToUint8()
				key[len(key)-1] = colSpec.Id
				if val.GetType() != cql_data_model.Expression {
					kvItems = append(kvItems, store.KVItem{Key: key, Value: val.Serialize()})
				} else if val.GetType() == cql_data_model.Null {
					kvItems = append(kvItems, store.KVItem{Key: key, Value: nil})
				} else {
					kvItems = append(kvItems, store.KVItem{Key: key, ValueExpr: val.String()})
				}
			}
		}
	}

	return kvItems, pKey, nil
}

func (tbl *CQLTable) RowToWriteOp(row *cql_data_model.Row, tableUUID uuid.UUID, cdResolver conflict_domain.ConflictDomainResolver) (*writeop.WriteOp, string, error) {

	kvItems, pk, err := tbl.RowToKVItems(*row)
	if err != nil {
		return nil, "", err
	}
	conflictDomain, err := cdResolver.KeyToConflictDomain(pk, tableUUID)
	if err != nil {
		return nil, "", err
	}
	cdKeyB64 := conflictDomain.B64()

	writeOp := &writeop.WriteOp{
		TableUUID: tableUUID,
		Items:     kvItems,
	}

	return writeOp, cdKeyB64, nil
}

/*************************************************************
* Read Helpers
* Here we take the identifying information for some data
* and construct the ReadOp to read this information into KVItems
* from the underlying KV Store
**************************************************************/

func (tbl *CQLTable) ReadPartition(partitionKeys []cql_data_model.LocusDBValue) *readop.ReadOp {
	prefixPartitionKey := make(store.ByteString, 0)
	for _, key := range partitionKeys {
		prefixPartitionKey = locusDBValueToKVKey(PartitionColumn.ToUint8(), key, prefixPartitionKey)
	}

	return &readop.ReadOp{
		TableUUID: tbl.tableSpec.TableUUID,
		ReadMode:  readop.PREFIX,
		SkipFirst: false,
		SkipLast:  false,
		StartKey:  prefixPartitionKey,
		EndKey:    nil,
	}
}

/*
 * Reads a portion of a partition bounded by clustering keys.
 * this can be a single row or contiguous slice of Rows
 */
func (tbl *CQLTable) ReadPartitionSlice(partitionKeys []cql_data_model.LocusDBValue, clusteringKeys []cql_data_model.LocusDBValue, clusteringComparators []cql_data_model.ComparatorType) *readop.ReadOp {
	prefixKey := make(store.ByteString, 0)
	for _, key := range partitionKeys {
		prefixKey = locusDBValueToKVKey(PartitionColumn.ToUint8(), key, prefixKey)
	}

	clusteringEqualities := 0
	for clusteringEqualities < len(clusteringComparators) && clusteringComparators[clusteringEqualities] == cql_data_model.EQUAL {
		prefixKey = locusDBValueToKVKey(ClusteringColumn.ToUint8(), clusteringKeys[clusteringEqualities], prefixKey)
		clusteringEqualities++
	}

	if clusteringEqualities == len(clusteringComparators) {
		return &readop.ReadOp{
			TableUUID: tbl.tableSpec.TableUUID,
			ReadMode:  readop.PREFIX,
			SkipFirst: false,
			SkipLast:  false,
			StartKey:  prefixKey,
			EndKey:    nil,
		}
	} else {
		skipFirst := false
		skipLast := false
		lowerLimit := make([]byte, len(prefixKey), len(prefixKey)*2)
		upperLimit := make([]byte, len(prefixKey), len(prefixKey)*2)
		switch clusteringComparators[clusteringEqualities] {
		case cql_data_model.GREATERTHAN:
			skipFirst = true
			fallthrough
		case cql_data_model.GREATERTHANEQUAL:
			lowerLimit = locusDBValueToKVKey(ClusteringColumn.ToUint8(), clusteringKeys[clusteringEqualities], prefixKey)
			upperLimit = append(prefixKey, NoCol.ToUint8())
		case cql_data_model.LESSTHAN:
			skipLast = true
			fallthrough
		case cql_data_model.LESSTHANEQUAL:
			lowerLimit = append(prefixKey, ClusteringColumn.ToUint8())
			upperLimit = locusDBValueToKVKey(ClusteringColumn.ToUint8(), clusteringKeys[clusteringEqualities], prefixKey)
		case cql_data_model.BETWEEN:
			lowerLimit = locusDBValueToKVKey(ClusteringColumn.ToUint8(), clusteringKeys[clusteringEqualities], prefixKey)
			upperLimit = locusDBValueToKVKey(ClusteringColumn.ToUint8(), clusteringKeys[clusteringEqualities+1], prefixKey)
		}
		return &readop.ReadOp{
			TableUUID: tbl.tableSpec.TableUUID,
			ReadMode:  readop.RANGE,
			SkipFirst: skipFirst,
			SkipLast:  skipLast,
			StartKey:  lowerLimit,
			EndKey:    upperLimit,
		}
	}
}

/*************************************************************
* Filter
* Here we translate KVItems into Rows and place these Rows
* into containers with metadata, such as display order
* and list of columns to show
**************************************************************/
func (tbl *CQLTable) TranslateKVReadResultsToRows(rawData []store.KVItem, colFilter map[uint8]struct{}, nonKeyColumnRelations map[uint8]*cql_data_model.ConstRelation) (*TableSnippet, error) {

	var err error
	var row cql_data_model.Row
	var prevKeySize int64 = 0
	prevRowKey := make([]byte, 0)
	suspectedKey := prevRowKey
	rowContainer := NewTableSnippet(tbl.tableSpec)

	for _, item := range rawData {
		if item.Key == nil {
			break
		}

		keyReader := bytes.NewReader(item.Key)
		valueReader := bytes.NewReader(item.Value)
		//suspectedKeyHash, err = translator.computeSuspectedKeyHash(prevKeySize, keyReader)
		suspectedKey, err = tbl.ReadSuspectedKey(prevKeySize, keyReader)

		if err != nil {
			log.Errorf("Error reading data from store: %v", err)
			return nil, err
		}

		if prevKeySize == 0 || !utils.AreByteStringsSame(prevRowKey, suspectedKey) {
			if prevKeySize != 0 {
				err := tbl.appendRowToContainerIfNeeded(row, rowContainer, nonKeyColumnRelations)
				if err != nil {
					return nil, err
				}
			}
			row, prevKeySize, prevRowKey, err = tbl.readKeyColumnsFromRawItem(keyReader, valueReader, colFilter)
			if err != nil {
				log.Errorf("Error reading data from store: %v", err)
				return nil, err
			}
		} else {
			_, err = keyReader.Seek(prevKeySize, io.SeekStart)
			if err != nil {
				log.Errorf("Error reading data from store: %v", err)
				return nil, err
			}
			var val cql_data_model.LocusDBValue
			var colId uint8
			val, colId, err = tbl.readRegularColumnFromRawItem(keyReader, valueReader)
			if err != nil {
				log.Errorf("Error reading regular column: %v", err)
				return nil, err
			}
			if row == nil {
				log.Errorf("Error reading regular column: row keys not read before reading regular column")
				return nil, errors.New("row keys not read before reading regular column")
			}

			if colFilter == nil {
				row[colId] = val
			} else if _, exists := colFilter[colId]; exists {
				row[colId] = val
			}
		}
	}
	if row != nil {
		err := tbl.appendRowToContainerIfNeeded(row, rowContainer, nonKeyColumnRelations)
		if err != nil {
			return nil, err
		}
	}

	return rowContainer, nil
}

func (tbl *CQLTable) appendRowToContainerIfNeeded(row cql_data_model.Row, rowContainer *TableSnippet, nonKeyColumnRelations map[uint8]*cql_data_model.ConstRelation) error {
	if len(nonKeyColumnRelations) > 0 {
		// we have some conditions to look at
		for colId, cr := range nonKeyColumnRelations {
			var colVal cql_data_model.LocusDBValue
			var exists bool
			if colVal, exists = row[colId]; !exists {
				colVal = cql_data_model.NewNullValue()
			}
			compareVal, err := cr.ConstVal.CastIfPossible(colVal.GetType())
			if err != nil {
				// cannot compare if we cannot cast
				if colVal.GetType() != cql_data_model.Null {
					return err
				} else {
					return nil
				}
			}
			compareResult, err := row[colId].Compare(compareVal)
			if err != nil {
				return err
			}

			emitRow := false
			switch cr.ComparatorType {
			case cql_data_model.EQUAL:
				if compareResult == 0 {
					emitRow = true
				}
			case cql_data_model.GREATERTHAN:
				if compareResult > 0 {
					emitRow = true
				}
			case cql_data_model.GREATERTHANEQUAL:
				if compareResult >= 0 {
					emitRow = true
				}
			case cql_data_model.LESSTHAN:
				if compareResult < 0 {
					emitRow = true
				}
			case cql_data_model.LESSTHANEQUAL:
				if compareResult <= 0 {
					emitRow = true
				}
			default:
				emitRow = false
			}
			if emitRow {
				rowContainer.Append(row)
			}
		}
	} else {
		rowContainer.Append(row)
	}
	return nil
}

func (tbl *CQLTable) readRegularColumnFromRawItem(keyReader, valReader *bytes.Reader) (cql_data_model.LocusDBValue, uint8, error) {
	p, err := keyReader.ReadByte()
	if err != nil {
		log.Errorf("Error reading data from store: %v", err)
		return nil, 0, err
	}
	var val cql_data_model.LocusDBValue
	keyType := LocusDBColumnKind(p)
	if keyType != RegularColumn {
		return nil, 0, fmt.Errorf("expected a column type, got type %d", keyType)
	}
	p, err = keyReader.ReadByte()
	if err != nil {
		log.Errorf("Error reading data from store: %v", err)
		return nil, 0, err
	}
	colId := p

	val, err = tbl.readValue(valReader, colId)

	if err != nil {
		log.Errorf("Error reading data from store: %v", err)
		return nil, 0, err
	}

	return val, colId, nil
}

func (tbl *CQLTable) readValue(valReader *bytes.Reader, colId uint8) (cql_data_model.LocusDBValue, error) {
	colspec := tbl.tableSpec.ColumnSpecs[colId]
	switch colspec.DataType {
	case cql_data_model.Int:
		return cql_data_model.NewIntFromReader(valReader)
	case cql_data_model.BigInt:
		return cql_data_model.NewBigIntValueFromReader(valReader)
	case cql_data_model.Float:
		return cql_data_model.NewFloatValueFromReader(valReader)
	case cql_data_model.Double:
		return cql_data_model.NewDoubleValueFromReader(valReader)
	case cql_data_model.Text:
		return cql_data_model.NewTextValueFromReader(valReader)
	case cql_data_model.Boolean:
		return cql_data_model.NewBooleanValueFromReader(valReader)
	default:
		return nil, ErrInvalidDataType
	}
}

func (tbl *CQLTable) readColumnIdFromRawItem(keyReader, valueReader *bytes.Reader) (uint8, error) {
	p, err := keyReader.ReadByte()
	if err != nil {
		log.Errorf("Error reading data from store: %v", err)
		return 0, err
	}
	colId := uint8(0) // the seq number of partition or cluster column
	for {             // read till we hit an error or regular column at the end of the stream
		colType := LocusDBColumnKind(p)

		p, err = keyReader.ReadByte()
		if err != nil {
			log.Errorf("Error reading data from store: %v", err)
			return 0, err
		}

		var dataType cql_data_model.LocusDBType
		var reader *bytes.Reader
		if colType == PartitionColumn || colType == ClusteringColumn {
			dataType = cql_data_model.LocusDBType(p)
			reader = keyReader
		} else if colType == RegularColumn {
			colId = p
			dataType = tbl.tableSpec.ColumnSpecs[colId].DataType
			reader = valueReader
		}

		switch dataType {
		case cql_data_model.Int:
			_, err = cql_data_model.NewIntFromReader(reader)
		case cql_data_model.BigInt:
			_, err = cql_data_model.NewBigIntValueFromReader(reader)
		case cql_data_model.Float:
			_, err = cql_data_model.NewFloatValueFromReader(reader)
		case cql_data_model.Double:
			_, err = cql_data_model.NewDoubleValueFromReader(reader)
		case cql_data_model.Text:
			_, err = cql_data_model.NewTextValueFromReader(reader)
		case cql_data_model.Boolean:
			_, err = cql_data_model.NewBooleanValueFromReader(reader)
		default:
			return 0, ErrInvalidDataType
		}

		if err != nil {
			log.Errorf("Error reading data from store: %v", err)
			return 0, err
		}

		if colType == RegularColumn {
			return colId, nil
		}

		p, err = keyReader.ReadByte()
		if err != nil {
			log.Errorf("Error reading data from store: %v", err)
			return 0, err
		}
		colId += 1 // PKs and CKs ids are consecutive, so we just increment colId until we hit regular column
	}
}

func (tbl *CQLTable) readKeyColumnsFromRawItem(keyReader, valueReader *bytes.Reader, colFilter map[uint8]struct{}) (cql_data_model.Row, int64, []byte, error) {
	row := make(cql_data_model.Row)
	p, err := keyReader.ReadByte()
	if err != nil {
		log.Errorf("Error reading data from store: %v", err)
		return nil, 0, nil, err
	}
	colId := uint8(0) // the seq number of partition or cluster column
	var kSize int64
	for { // read till we hit an error or regular column at the end of the stream
		var val cql_data_model.LocusDBValue
		colType := LocusDBColumnKind(p)

		p, err = keyReader.ReadByte()
		if err != nil {
			log.Errorf("Error reading data from store: %v", err)
			return nil, 0, nil, err
		}

		var dataType cql_data_model.LocusDBType
		var reader *bytes.Reader
		if colType == PartitionColumn || colType == ClusteringColumn {
			dataType = cql_data_model.LocusDBType(p)
			reader = keyReader
		} else if colType == RegularColumn {
			colId = p
			dataType = tbl.tableSpec.ColumnSpecs[colId].DataType
			reader = valueReader
		}

		switch dataType {
		case cql_data_model.Int:
			val, err = cql_data_model.NewIntFromReader(reader)
		case cql_data_model.BigInt:
			val, err = cql_data_model.NewBigIntValueFromReader(reader)
		case cql_data_model.Float:
			val, err = cql_data_model.NewFloatValueFromReader(reader)
		case cql_data_model.Double:
			val, err = cql_data_model.NewDoubleValueFromReader(reader)
		case cql_data_model.Text:
			val, err = cql_data_model.NewTextValueFromReader(reader)
		case cql_data_model.Boolean:
			val, err = cql_data_model.NewBooleanValueFromReader(reader)
		default:
			return nil, 0, nil, ErrInvalidDataType
		}

		if err != nil {
			log.Errorf("Error reading data from store: %v", err)
			return nil, 0, nil, err
		}

		done := false
		if colType == PartitionColumn {
			kSize = keyReader.Size() - int64(keyReader.Len())
		} else if colType == ClusteringColumn {
			kSize = keyReader.Size() - int64(keyReader.Len())
		} else if colType == RegularColumn {
			done = true // regular column should be at the very end of this byte stream. we are good to break
		}

		if colFilter == nil {
			row[colId] = val
		} else if _, exists := colFilter[colId]; exists {
			row[colId] = val
		}

		if done {
			break
		}

		p, err = keyReader.ReadByte()
		if err != nil {
			log.Errorf("Error reading data from store: %v", err)
			return nil, 0, nil, err
		}
		colId += 1 // PKs and CKs ids are consecutive, so we just increment colId until we hit regular column
	}

	//keyHash, keyHashErr := translator.computeSuspectedKeyHash(kSize, keyReader)
	key, keyErr := tbl.ReadSuspectedKey(kSize, keyReader)
	if keyErr != nil {
		log.Errorf("Error reading data from store: %v", err)
		return nil, 0, nil, err
	}

	return row, kSize, key, nil
}

func (tbl *CQLTable) ReadSuspectedKey(keySize int64, keyReader *bytes.Reader) ([]byte, error) {
	if keySize > 0 {
		k := make([]byte, keySize)
		_, err := keyReader.ReadAt(k, 0)
		if err != nil {
			log.Errorf("Error reading data from store: %v", err)
			return nil, err
		}

		return k, nil
	}
	return nil, nil
}
