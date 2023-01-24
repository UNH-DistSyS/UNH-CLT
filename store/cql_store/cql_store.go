package cql_store

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"sync"

	"github.com/UNH-DistSyS/UNH-CLT/conflict_domain"
	"github.com/UNH-DistSyS/UNH-CLT/log"
	"github.com/UNH-DistSyS/UNH-CLT/parsers/locus_expression/expression_machine"
	"github.com/UNH-DistSyS/UNH-CLT/replication/operations"
	"github.com/UNH-DistSyS/UNH-CLT/replication/operations/minitx"
	"github.com/UNH-DistSyS/UNH-CLT/replication/operations/readop"
	"github.com/UNH-DistSyS/UNH-CLT/replication/operations/writeop"
	"github.com/UNH-DistSyS/UNH-CLT/store"
	"github.com/UNH-DistSyS/UNH-CLT/store/cql_store/cql_data_model"
	"github.com/UNH-DistSyS/UNH-CLT/store/kv_store"
	"github.com/google/uuid"
)

func init() {
	gob.Register(LocusDBTableSpec{})
	gob.Register(&TableSnippet{})

	// register value types, since we pass typed responses back to client
	gob.Register(cql_data_model.IntValue{})
	gob.Register(cql_data_model.BigIntValue{})
	gob.Register(cql_data_model.FloatValue{})
	gob.Register(cql_data_model.DoubleValue{})
	gob.Register(cql_data_model.TextValue{})
	gob.Register(cql_data_model.BooleanValue{})
	gob.Register(cql_data_model.NullValue{})
	gob.Register(cql_data_model.ExpressionValue{})
}

/*******************************************************************************
* Hardcoded definitions of some system-level tables
* 1: Columns table - lists columns in each table
********************************************************************************/
var locusDBColumnsUUID, _ = uuid.Parse("4c2ad390-b73b-11ea-aca5-bd0d82bb57cd")
var locusDBColumnsColumns = []*LocusDBColumnSpec{
	{
		Name:     "tableId",
		DataType: cql_data_model.Text,
		Id:       0,
		OrderNum: 0,
		Kind:     PartitionColumn,
	},
	{
		Name:     "columnName",
		DataType: cql_data_model.Text,
		Id:       1,
		OrderNum: 1,
		Kind:     ClusteringColumn,
	},
	{
		Name:     "columnId",
		DataType: cql_data_model.Int,
		Id:       2,
		OrderNum: 2,
		Kind:     RegularColumn,
	},
	{
		Name:     "columnDataType",
		DataType: cql_data_model.Int,
		Id:       3,
		OrderNum: 3,
		Kind:     RegularColumn,
	},
	{
		Name:     "columnOrder",
		DataType: cql_data_model.Int,
		Id:       4,
		OrderNum: 4,
		Kind:     RegularColumn,
	},
	{
		Name:     "columnType",
		DataType: cql_data_model.Int,
		Id:       5,
		OrderNum: 5,
		Kind:     RegularColumn,
	},
}
var locusDBColumnsSchema = NewLocusDBTableSpec(locusDBColumnsUUID, "locus_columns", locusDBColumnsColumns)

type CQLStore struct {
	*kv_store.KVStore
	conflictDomainResolver conflict_domain.ConflictDomainResolver
	tables                 map[uuid.UUID]*CQLTable
	sync.RWMutex
}

func NewCQLStore(store *kv_store.KVStore, cdResolver conflict_domain.ConflictDomainResolver) *CQLStore {
	log.Infof("Creating CQL store at node %v", store.NodeId)
	tc := &CQLStore{tables: make(map[uuid.UUID]*CQLTable), KVStore: store, conflictDomainResolver: cdResolver}
	return tc
}

func (s *CQLStore) Initialize() error {
	log.Infof("Initializing CQL Store on node %v", s.ID)
	// first initialize the underlying kv-store
	err := s.KVStore.Initialize()
	if err != nil {
		log.Errorf("Error Initializing Underlying KVStore: %v", err)
		return err
	}
	// create columns cql table
	_, err = s.CreateTable(locusDBColumnsSchema.TableName, locusDBColumnsSchema.TableUUID)
	if err != nil {
		log.Errorf("Could not create columns table: %v", err)
		return err
	}
	columnsTable := CQLTable{tableSpec: locusDBColumnsSchema}
	s.setTable(locusDBColumnsSchema.TableUUID, &columnsTable)
	// now read the table to see what cql table specs we can have
	tables := s.GetTableList()

	for _, tableName := range tables {
		spec, tableId, err := s.readColumnsTable(tableName)
		if err != nil {
			log.Errorf("Error Initializing table %s: %v", tableName, err)
			return err
		}
		if spec != nil {
			// add to list of CQL tables if there is a spec
			s.setTable(*tableId, &CQLTable{tableSpec: spec})
		}
	}
	return nil
}

func (s *CQLStore) GetConflictDomainResolver(cdStoreHeaderByte uint8) conflict_domain.ConflictDomainResolver {
	// since the stores are stacked on top of each other, we have to check if the conflict domain is for this store layer
	if cdStoreHeaderByte == conflict_domain.ConflictResolverCql {
		return s.conflictDomainResolver
	} else {
		return s.KVStore.GetConflictDomainResolver(cdStoreHeaderByte)
	}
}

func (s *CQLStore) ApplyCommand(command *store.Command, shortNonMutating bool) (*store.CommandResult, error) {
	// CQL Store sits on top of the KV. In case we receive a command for KV store, pass it on
	if command.TargetStore == store.KV {
		return s.KVStore.ApplyCommand(command, shortNonMutating)
	}
	switch command.OperationType {
	case store.WRITE:
		// this command falls through directly to the KV layer, as writes are expressed as a KV write
		return s.KVStore.ApplyCommand(command, shortNonMutating)
	case store.UPDATE:
		switch command.Operation.(type) {
		case operations.CqlUpdateOp:
			updateOp := command.Operation.(operations.CqlUpdateOp)
			log.Debugf("Handling update in CQL store: %v", updateOp)
			_, err := s.applyUpdateOp(updateOp, command.ConflictDomainKey, command.Version)
			if err != nil {
				return nil, err
			}
			return command.CreateResult(nil, updateOp.ReadSpec.TableUUID), nil
		default:
			return nil, store.ErrIncorrectReplicationOp
		}
	case store.DELETE:
		switch command.Operation.(type) {
		case operations.DeleteOp:
			deleteOp := command.Operation.(operations.DeleteOp)
			log.Debugf("Handling slow delete in CQL store: %v", deleteOp)
			if deleteOp.ColumnNames == nil || len(deleteOp.ColumnNames) == 0 {
				// we can fall through to the KV store handling the delete, since we are deleting full rows
				// identified by the readop inside the deleteop
				return s.KVStore.ApplyCommand(command, shortNonMutating)
			} else {
				err := s.applyDeleteOp(deleteOp, command.ConflictDomainKey, command.Version)
				if err != nil {
					return nil, err
				}
				// we still create a result to propagate TableUUID down to the request handling layer
				return command.CreateResult(nil, deleteOp.ReadSpec.TableUUID), nil
			}
		default:
			return nil, store.ErrIncorrectReplicationOp
		}
	case store.READ:
		// this command goes directly to the KV layer, as reads are expressed as a KV read
		switch command.Operation.(type) {
		case readop.CQLReadOp:
			if shortNonMutating {
				// this is a read-only operation that does not return to client, so we do not actually have to do the read
				return nil, nil
			}
			cqlReadOp := command.Operation.(readop.CQLReadOp)

			var err error

			if command.Version == 0 && len(command.ConflictDomainKey) > 0 {
				command.Version, err = s.ReadVersionOfStoredConflictDomain(command.ConflictDomainKey)
				if err != nil {
					return nil, err
				}
			}

			cmdKVRead := store.Command{
				CommandID:         command.CommandID,
				ConflictDomainKey: command.ConflictDomainKey,
				OperationType:     store.READ,
				Operation:         *cqlReadOp.ReadOp,
				TargetStore:       store.KV,
				Version:           command.Version,
			}

			kvResult, err := s.KVStore.ApplyCommand(&cmdKVRead, shortNonMutating)
			if err != nil {
				log.Errorf("Error reading from KV Store: %v", err)
				return nil, err
			}

			if kvResult == nil {
				return nil, nil
			}

			// however, we then need to transform KV read to CQL Rows
			// while doing so, we also need to filter out rows that do not match non-key column conditions
			kvReads := kvResult.Result.([]store.KVItem)
			table := s.GetTable(cqlReadOp.TableUUID)
			//command.
			if table == nil {
				// no table, so we cannot do translation
				// normally we should not be here though, as CQL request has a table check earlier in the processing flow
				return nil, store.ErrTableNotFound
			}

			colFilter := make(map[uint8]struct{}, len(cqlReadOp.ColIds))
			for _, colId := range cqlReadOp.ColIds {
				colFilter[colId] = struct{}{}
			}

			rows, err := table.TranslateKVReadResultsToRows(kvReads, colFilter, cqlReadOp.NonKeyColumnConditions)
			if err != nil {
				return nil, err
			}
			return command.CreateResult(rows, cqlReadOp.TableUUID), nil
		default:
			return nil, store.ErrIncorrectReplicationOp
		}

	case store.DDL_WRITE:
		switch command.Operation.(type) {
		case LocusDBTableSpec:
			// here we have a command that changes the schema
			// so we expect a schema in the operation
			tableSchema := command.Operation.(LocusDBTableSpec)
			tableSchema.InitTableSpec()
			//check if table exists
			exists, existsErr := s.checkTableExist(&tableSchema)
			if tableSchema.SkipCreateIfExists && exists {
				log.Debugf("Store %v: table %v exists, skipping create", s.ID, tableSchema.TableName)
				return command.CreateResult(nil, tableSchema.TableUUID), nil
			} else if exists {
				return nil, existsErr
			}
			// check the schema
			err := s.checkTableSpec(&tableSchema)
			if err != nil {
				log.Errorf("Error Creating a CQL Table: %v", err)
				return nil, err
			}
			err = s.createCQLTable(&tableSchema, command.Version)
			if err != nil {
				log.Errorf("Error Creating a CQL Table: %v", err)
				return nil, err
			}

			return command.CreateResult(nil, tableSchema.TableUUID), nil
		default:
			return nil, store.ErrIncorrectReplicationOp
		}
	case store.MINI_TX:
		// TODO: same-partition transactions
		switch command.Operation.(type) {
		case minitx.MiniTx:
			log.Debugf("Its a Mini transaction! Node: %v", s.ID)
		default:
			return nil, store.ErrIncorrectReplicationOp
		}
	case store.NOOP:
		return nil, nil
	}

	return nil, errors.New("unknown store operation")
}

func (s *CQLStore) GetTable(tableId uuid.UUID) *CQLTable {
	s.RLock()
	defer s.RUnlock()
	return s.tables[tableId]
}

func (s *CQLStore) setTable(tableId uuid.UUID, table *CQLTable) {
	s.Lock()
	defer s.Unlock()
	s.tables[tableId] = table
}

func (s *CQLStore) GetTableByName(name string) *CQLTable {
	tblUUID := s.ResolveTableId(name)
	if tblUUID != nil {
		return s.GetTable(*tblUUID)
	}
	return nil
}

/****************************************************************************************************
 * Reads the columns table from underlying KV store, and translates it to schema
 ***************************************************************************************************/
// TODO: move to LocusDBTableSpec
func (s *CQLStore) readColumnsTable(tableName string) (*LocusDBTableSpec, *uuid.UUID, error) {
	columnsTable := s.GetTable(locusDBColumnsSchema.TableUUID)
	tableId := s.ResolveTableId(tableName)
	columnsPk := cql_data_model.NewTextValue(tableId.String())
	columnsTableReadOp := columnsTable.ReadPartition([]cql_data_model.LocusDBValue{columnsPk})

	//read directly from table
	kvItems, err := s.KVStore.GetTable(locusDBColumnsSchema.TableUUID).Read(columnsTableReadOp)
	if err != nil {
		log.Errorf("Error reading Columns table: %v", err)
		return nil, nil, err
	}

	if len(kvItems) == 0 {
		// table is not CQL, as its  column schema is not recorded in columns table
		return nil, nil, nil
	}

	var rows *TableSnippet
	rows, err = columnsTable.TranslateKVReadResultsToRows(kvItems, nil, nil)
	if err != nil {
		log.Errorf("Error translating KV column data to Rows: %v", err)
		return nil, nil, err
	}
	colSpecs := make([]*LocusDBColumnSpec, 0, len(rows.Rows))
	var tableSpec *LocusDBTableSpec
	// each row corresponds ot one column definition
	for _, row := range rows.Rows {
		tableId, err := uuid.Parse(row[0].(*cql_data_model.TextValue).Val)
		if err != nil {
			log.Errorf("Error reading Columns table. Could not parse tableId: %v", err)
			return nil, nil, err
		}
		columnName := row[1].(*cql_data_model.TextValue).Val
		columnId := uint8(row[2].(*cql_data_model.IntValue).Val)
		colDataType := cql_data_model.LocusDBType(row[3].(*cql_data_model.IntValue).Val)
		columnOrder := uint8(row[4].(*cql_data_model.IntValue).Val)
		columnType := LocusDBColumnKind(row[5].(*cql_data_model.IntValue).Val)
		colSpecs = append(colSpecs, &LocusDBColumnSpec{
			Name:     columnName,
			DataType: colDataType,
			Id:       columnId,
			OrderNum: columnOrder,
			Kind:     columnType,
		})
		tableSpec = NewLocusDBTableSpec(tableId, tableName, colSpecs)
	}
	return tableSpec, tableId, nil
}

func (s *CQLStore) specToColumnsTableRows(spec *LocusDBTableSpec) []cql_data_model.Row {
	rows := make([]cql_data_model.Row, len(spec.ColumnSpecs))
	j := 0
	for _, colspec := range spec.ColumnSpecs {
		row := make(map[uint8]cql_data_model.LocusDBValue, 6) // TODO: make this 6 a defined constant
		row[0] = cql_data_model.NewTextValue(spec.TableUUID.String())
		row[1] = cql_data_model.NewTextValue(colspec.Name)
		row[2] = cql_data_model.NewInt(int32(colspec.Id))
		row[3] = cql_data_model.NewInt(int32(colspec.DataType.GetVal()))
		row[4] = cql_data_model.NewInt(int32(colspec.OrderNum))
		row[5] = cql_data_model.NewInt(int32(colspec.Kind))
		rows[j] = row
		j += 1
	}
	return rows
}

func (s *CQLStore) createCQLTable(spec *LocusDBTableSpec, ddlVersion uint64) error {
	// to create CQL Table, we need to create underlying KV-table first

	if created, err := s.CreateTable(spec.TableName, spec.TableUUID); created {
		// once the KV table is there, we need to add schema to columns table
		// easiest way to do it, is to convert the spec to KV and write directly
		columnsTable := s.GetTable(locusDBColumnsSchema.TableUUID)
		//columnsTable.RowsToKVItems()
		rows := s.specToColumnsTableRows(spec)
		kvitems, err := columnsTable.RowsToKVItems(rows)
		if err != nil {
			return err
		}
		// now create a write command to pass to the underlying KV store
		writeOp := writeop.WriteOp{
			TableUUID: locusDBColumnsUUID,
			Items:     kvitems,
		}
		writecmd := &store.Command{
			CommandID:         0,
			ConflictDomainKey: cql_data_model.NewTextValue(spec.TableUUID.String()).Serialize(),
			OperationType:     store.WRITE,
			Operation:         writeOp,
			TargetStore:       store.CQL,
			Version:           ddlVersion,
		}
		_, err = s.KVStore.ApplyCommand(writecmd, true)
		if err != nil {
			return err
		}
		tbl := &CQLTable{tableSpec: spec}
		s.setTable(spec.TableUUID, tbl)
	} else if err != nil {
		return err
	} else {
		return fmt.Errorf("error: Table %v with Id %v already exists", spec.TableName, spec.TableUUID)
	}
	return nil
}

func (s *CQLStore) checkTableExist(schema *LocusDBTableSpec) (bool, error) {
	tbl := s.GetTableByName(schema.TableName)
	if tbl != nil {
		return true, fmt.Errorf("table %v already exists with Id %v", schema.TableName, tbl.GetSchema().GetTableUUID())
	}

	if s.TableExists(schema.TableUUID) {
		return true, fmt.Errorf("table Id %v already exists", schema.TableUUID)
	}
	return false, nil
}

func (s *CQLStore) checkTableSpec(schema *LocusDBTableSpec) error {
	if schema.pkCount == 0 {
		return fmt.Errorf("table must have at least on partition key componen")
	}

	ids := make(map[uint8]bool)
	for _, colSpec := range schema.ColumnSpecs {
		if colSpec.DataType == cql_data_model.Expression {
			return fmt.Errorf("expression datatype is not valid for columns")
		}

		if _, exists := ids[colSpec.Id]; exists {
			return fmt.Errorf("column ids must be uniques - already have colid %d", colSpec.Id)
		}
		ids[colSpec.Id] = true
	}

	return nil
}

func (s *CQLStore) applyUpdateOp(updateOp operations.CqlUpdateOp, cdKey store.ByteString, updateLogSlot uint64) (int, error) {
	table := s.GetTable(updateOp.ReadSpec.TableUUID)
	if table == nil {
		// no table, so we cannot do translation
		// normally we should not be here though, as CQL request has a table check earlier in the processing flow
		return 0, store.ErrTableNotFound
	}

	// first, do the read
	readCmd := &store.Command{
		CommandID:         0,
		ConflictDomainKey: nil,
		OperationType:     store.READ,
		Operation:         updateOp.ReadSpec,
		TargetStore:       store.CQL,
	}
	readRows, err := s.ApplyCommand(readCmd, false)
	if err != nil {
		return 0, err
	}

	// now for every row we found we can apply the update
	em := expression_machine.NewExpressionMachine()
	tableSnip := readRows.Result.(*TableSnippet)
	var writeOp *writeop.WriteOp
	for _, row := range tableSnip.Rows {
		// copy update row over read row
		rowExpressions := make(map[uint8]cql_data_model.LocusDBValue)
		for colId := range row {
			em.AddToMemTable(tableSnip.TableSpec.ColumnSpecs[colId].Name, row[colId])
			if updateOp.WriteRow[colId] != nil {
				if updateOp.WriteRow[colId].GetType() == cql_data_model.Expression {
					rowExpressions[colId] = updateOp.WriteRow[colId]
				} else {
					row[colId] = updateOp.WriteRow[colId]
				}
			}
		}
		for colId := range rowExpressions {
			val, err := em.RunExpression(updateOp.WriteRow[colId].String())
			if len(err) > 0 {
				return 0, err[0]
			}
			valIntype, castErr := val.CastIfPossible(tableSnip.TableSpec.ColumnSpecs[colId].DataType)
			if castErr != nil {
				return 0, castErr
			}
			row[colId] = valIntype
		}
		rowWrite, _, err := table.RowToWriteOp(&row, table.tableSpec.TableUUID, s.conflictDomainResolver)
		if err != nil {
			return 0, err
		}

		if writeOp == nil {
			writeOp = rowWrite
		} else {
			writeOp.Items = append(writeOp.Items, rowWrite.Items...)
		}
	}

	if updateLogSlot > 0 && writeOp != nil {
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, updateLogSlot)
		item := store.KVItem{Key: cdKey, Value: b}
		writeOp.Items = append(writeOp.Items, item)
	}

	// directly access the underlying KV-table
	err = s.KVStore.GetTable(table.tableSpec.TableUUID).Write(writeOp)
	if err != nil {
		return 0, err
	}

	return len(tableSnip.Rows), nil
}

func (s *CQLStore) addRowToExpressionMachineMemTable(em expression_machine.ExpressionMachine, row cql_data_model.Row, colspecs []LocusDBColumnSpec) {
	for colId, colVal := range row {
		em.AddToMemTable(colspecs[colId].Name, colVal)
	}
}

func (s *CQLStore) applyDeleteOp(deleteOp operations.DeleteOp, cdKey store.ByteString, deleteLogSlot uint64) error {
	table := s.GetTable(deleteOp.ReadSpec.TableUUID)
	if table == nil {
		// no table, so we cannot do translation
		// normally we should not be here though, as CQL request has a table check earlier in the processing flow
		return store.ErrTableNotFound
	}

	// first, do a simple KV read
	kvItems, err := s.KVStore.GetTable(deleteOp.ReadSpec.TableUUID).Read(&deleteOp.ReadSpec)
	if err != nil {
		return err
	}

	deleteWriteOp := &writeop.WriteOp{
		TableUUID: table.tableSpec.TableUUID,
		Items:     make([]store.KVItem, 0),
	}

	if len(deleteOp.NonKeyColumnConditions) > 0 {
		// we need to delete with non-key column filtering, which we cannot do with a simple KV read
		colFilter := make(map[uint8]struct{}, len(deleteOp.ColumnNames))
		for _, colName := range deleteOp.ColumnNames {
			if colId, exists := table.tableSpec.columnNameToId[colName]; exists {
				colFilter[colId] = struct{}{}
			} else {
				return fmt.Errorf("column %v not found in table %v", colName, table.tableSpec.TableName)
			}
		}

		rows, err := table.TranslateKVReadResultsToRows(kvItems, colFilter, deleteOp.NonKeyColumnConditions)
		if err != nil {
			return err
		}

		// now we can iterate thorugh the rows and delete them
		for _, row := range rows.Rows {
			kvs, _, err := table.RowToKVItems(row)
			if err != nil {
				return err
			}

			for _, kv := range kvs {
				kv.Value = nil
				deleteWriteOp.Items = append(deleteWriteOp.Items, kv)
			}
		}

	} else {
		// when we do not need filtering on non-key columns, we can just do KV read and enumarate KV cells to delete

		// now find columns we want to delete, and construct the writeOp setting them to nil
		// first lets find column ids of what we want to delete
		deleteColIds := make(map[uint8]bool, len(deleteOp.ColumnNames))
		for _, columnName := range deleteOp.ColumnNames {
			deleteColIds[table.tableSpec.columnNameToId[columnName]] = true
		}

		for _, item := range kvItems {
			keyReader := bytes.NewReader(item.Key)
			valueReader := bytes.NewReader(item.Value)

			colId, err := table.readColumnIdFromRawItem(keyReader, valueReader)
			if err != nil {
				return err
			}

			if _, exists := deleteColIds[colId]; exists {
				// delete this item
				item.Value = nil
				deleteWriteOp.Items = append(deleteWriteOp.Items, item)
			}
		}
	}

	if deleteLogSlot > 0 {
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, deleteLogSlot)
		item := store.KVItem{Key: cdKey, Value: b}
		deleteWriteOp.Items = append(deleteWriteOp.Items, item)
	}
	// directly access the underlying KV-table
	return s.KVStore.GetTable(table.tableSpec.TableUUID).Write(deleteWriteOp)
}

func (s *CQLStore) ReadVersionOfStoredConflictDomain(conflictDomainKey store.ByteString) (uint64, error) {
	cdResolver := s.GetConflictDomainResolver(conflictDomainKey[0])
	if cdResolver == nil {
		return 0, errors.New("conflict domain resolver for this conflict domain is missing")
	}
	tblUUIDs, err := cdResolver.ConflictDomainKeyToTableUUIDs(conflictDomainKey)
	if err != nil {
		return 0, err
	}
	maxversion := uint64(0)
	for _, tblUUID := range tblUUIDs {
		// directly access each table's CDVersion record
		cdVersion, err := s.KVStore.GetTable(tblUUID).ReadConflictDomainVersion(conflictDomainKey)
		if err != nil {
			return 0, err
		}
		if maxversion < cdVersion {
			maxversion = cdVersion
		}
	}
	return maxversion, err
}
