package cql_request_handler

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/UNH-DistSyS/UNH-CLT/config"
	"github.com/UNH-DistSyS/UNH-CLT/conflict_domain"
	"github.com/UNH-DistSyS/UNH-CLT/core"
	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/log"
	"github.com/UNH-DistSyS/UNH-CLT/netwrk"
	"github.com/UNH-DistSyS/UNH-CLT/parsers/cql_parser"
	"github.com/UNH-DistSyS/UNH-CLT/replication/operations"
	"github.com/UNH-DistSyS/UNH-CLT/replication/operations/minitx"
	"github.com/UNH-DistSyS/UNH-CLT/replication/operations/readop"
	"github.com/UNH-DistSyS/UNH-CLT/replication/operations/writeop"
	"github.com/UNH-DistSyS/UNH-CLT/request"
	"github.com/UNH-DistSyS/UNH-CLT/store"
	"github.com/UNH-DistSyS/UNH-CLT/store/cql_store"
	"github.com/UNH-DistSyS/UNH-CLT/store/cql_store/cql_data_model"
	"github.com/google/uuid"
)

type readMeta struct {
	namesOrAliases    []string
	colTypes          []cql_data_model.LocusDBType
	readop            *readop.CQLReadOp
	ConflictDomainKey store.ByteString
}

type requestWrapper struct {
	originalCqlRequest  request.CqlRequest
	replicationRequests []request.OperationReplicationRequest
	readMetas           map[ids.CommandID]*readMeta // this maps replicationRequest to a read meta so we can do column filtering, etc.

	replies      []request.OperationReplicationReply
	recvdReplies int
}

type CqlRequestHandler struct {
	ids.ID                         // must have an identity
	cfg                            *config.Config
	cqlStore                       *cql_store.CQLStore
	replicationOperationDispatcher core.OperationDispatcher
	cdResolver                     conflict_domain.ConflictDomainResolver
	lastCommandID                  uint64
	lastRequestWrapperID           uint64
	canServe                       bool
}

func NewCqlRequestHandler(identity ids.ID,
	cfg *config.Config,
	replicationOperationDispatcher core.OperationDispatcher,
	cqlStore *cql_store.CQLStore,
	cdResolver conflict_domain.ConflictDomainResolver) *CqlRequestHandler {
	requestHandler := new(CqlRequestHandler)
	requestHandler.ID = identity
	requestHandler.cfg = cfg
	requestHandler.replicationOperationDispatcher = replicationOperationDispatcher
	requestHandler.cqlStore = cqlStore
	requestHandler.lastCommandID = uint64(ids.NewCommandId(identity, 0))
	requestHandler.canServe = false
	requestHandler.cdResolver = cdResolver
	log.Infof("Creating Cql Client Request Handler on node id = %v \n", requestHandler.ID)

	return requestHandler
}

func (rh *CqlRequestHandler) Start() {
	rh.canServe = true
}

/*************************************
	Handles
 ************************************/

// HandleClientRequest registers with httpserv or netman to receive requests from clients
// this is where we do he translation between client_driver request and replication requests
// client_driver request is translated into either a read, write or DDL write replication operation
func (rh *CqlRequestHandler) HandleClientRequest(ctx context.Context, req interface{}) {
	m := req.(request.CqlRequest)
	// put request_id into the header
	meta := ctx.Value(netwrk.CtxMeta)
	if meta == nil {
		meta = &core.ContextMeta{
			RequestID: m.RequestId,
		}
		ctx = context.WithValue(ctx, netwrk.CtxMeta, meta)
	} else {
		switch meta.(type) {
		case *core.ContextMeta:
			ctxmeta := meta.(*core.ContextMeta)
			ctxmeta.RequestID = m.RequestId
			ctx = context.WithValue(ctx, netwrk.CtxMeta, ctxmeta)
		default:
			err := errors.New("server is not yet ready to serve requests")
			log.Errorf(err.Error())
			rh.replyError(m, err)
			return
		}
	}

	log.Debugf("CQLRequestHandler %s received %v\n", rh.ID, m)
	if !rh.canServe || rh.cdResolver == nil {
		rh.replyError(m, errors.New("CQL server is not yet ready to serve client requests"))
		return
	}
	errs := make([]string, 0)
	inserts := m.CqlInserts
	updates := m.CqlUpdates
	selects := m.CqlSelects
	deletes := m.CqlDeletes
	creates := m.CqlCreateTbls
	if m.ParseAtSrvr {
		// we need to parse the queries first
		cqlListener, cqlErrorListener := cql_parser.ParseCQL(m.CqlQueries)
		if !cqlErrorListener.HasErrors() {
			inserts = cqlListener.GetInserts()
			updates = cqlListener.GetUpdates()
			deletes = cqlListener.GetDeletes()
			selects = cqlListener.GetSelects()
			creates = cqlListener.GetCreateTables()
		} else {
			errs = append(errs, cqlErrorListener.GetErrorsStrings()...)
		}
	}

	// Create Tables that are not part of data operations, run them separately
	if len(creates) > 0 {
		replyChan := make(chan request.OperationReplicationReply, len(creates))
		reqWrapper := rh.compileDDLReplicationOperations(creates, &m, replyChan)
		go rh.receiveReplies(ctx, reqWrapper, replyChan)
		for _, replicationReq := range reqWrapper.replicationRequests {
			log.Debugf("Node %v Enqueueing replication request %v in request handler", rh.ID, replicationReq)
			rh.replicationOperationDispatcher.EnqueueOperation(ctx, replicationReq)
		}
	}

	CDOpsCounter := make(map[string]int)
	writeOps := make(map[string]map[int]writeop.WriteOp)
	updateOps := make(map[string]map[int]operations.CqlUpdateOp)
	slowDeletes := make(map[string]map[int]operations.DeleteOp)
	//useMiniTx := make(map[string]bool, 0)

	for _, insert := range inserts {
		// hasExpression is ignored on insert for now. CQL does not allow expressions like that anyway
		row, tableUUID, _, err := rh.insertToRow(insert)
		if err != nil {
			log.Errorf("Node %v CqlRequestError: %v", rh.ID, err)
			errs = append(errs, err.Error())
		}

		tbl := rh.cqlStore.GetTable(tableUUID)
		if tbl == nil {
			errs = append(errs, store.ErrTableNotFound.Error())
		} else {
			writeOp, conflictDomainB64, err := tbl.RowToWriteOp(&row, tableUUID, rh.cdResolver)
			if err != nil {
				log.Errorf("Node %v CqlRequestError: %v", rh.ID, err)
				errs = append(errs, err.Error())
			} else {
				if writeOps[conflictDomainB64] == nil {
					writeOps[conflictDomainB64] = make(map[int]writeop.WriteOp, 0)
				}
				writeOps[conflictDomainB64][CDOpsCounter[conflictDomainB64]] = *writeOp
				CDOpsCounter[conflictDomainB64]++
			}
		}
	}

	for _, update := range updates {
		row, tableUUID, needUpdateOp, err := rh.updateToRow(update)
		if err != nil {
			log.Errorf("Node %v CqlRequestError: %v", rh.ID, err)
			errs = append(errs, err.Error())
		}

		if needUpdateOp {
			// this update requires an updateOp, since it is not setting a value to a row directly, and instead
			// either requires updating multiple rows or uses an expression to update columns or both
			readInfo, err := rh.selectToReadInfo(update.ToSelectQuery())
			if err != nil {
				log.Errorf("Node %v CqlRequestError: %v", rh.ID, err)
				errs = append(errs, err.Error())
			} else {
				uop := operations.CqlUpdateOp{
					ReadSpec: *readInfo.readop,
					WriteRow: row,
				}
				conflictDomainB64 := readInfo.ConflictDomainKey.B64()
				if updateOps[conflictDomainB64] == nil {
					updateOps[conflictDomainB64] = make(map[int]operations.CqlUpdateOp, 0)
				}
				updateOps[conflictDomainB64][CDOpsCounter[conflictDomainB64]] = uop
				CDOpsCounter[conflictDomainB64]++
			}
		} else {
			tbl := rh.cqlStore.GetTable(tableUUID)
			if tbl == nil {
				errs = append(errs, store.ErrTableNotFound.Error())
			} else {
				writeOp, conflictDomainB64, err := tbl.RowToWriteOp(&row, tableUUID, rh.cdResolver)
				if err != nil {
					log.Errorf("Node %v CqlRequestError: %v", rh.ID, err)
					errs = append(errs, err.Error())
				} else {
					if writeOps[conflictDomainB64] == nil {
						writeOps[conflictDomainB64] = make(map[int]writeop.WriteOp, 0)
					}
					writeOps[conflictDomainB64][CDOpsCounter[conflictDomainB64]] = *writeOp
					CDOpsCounter[conflictDomainB64]++
				}
			}
		}
	}

	for _, del := range deletes {
		// hasExpression is ignored on insert for now. CQL does not allow expressions like that anyway
		row, tableUUID, err := rh.deleteToRow(del)
		if err != nil {
			errs = append(errs, err.Error())
		}

		tbl := rh.cqlStore.GetTable(tableUUID)
		if tbl == nil {
			errs = append(errs, store.ErrTableNotFound.Error())
		} else {
			if rh.cqlStore.GetTable(tableUUID).IsRowValid(row) {
				log.Debugf("Attempting a fast delete with writeOp")
				writeOp, conflictDomainB64, err := tbl.RowToWriteOp(&row, tableUUID, rh.cdResolver)
				if err != nil {
					log.Errorf("Node %v CqlRequestError: %v", rh.ID, err)
					errs = append(errs, err.Error())
				} else {
					if writeOps[conflictDomainB64] == nil {
						writeOps[conflictDomainB64] = make(map[int]writeop.WriteOp, 0)
					}
					writeOps[conflictDomainB64][CDOpsCounter[conflictDomainB64]] = *writeOp
					CDOpsCounter[conflictDomainB64]++
				}
			} else {
				log.Debugf("Attempting a slow delete")
				// the delete constraint does not represent a valid row. It may identify 2+ rows or a partition
				// so we need to do a slow delete instead of writing nils to the row
				// slow delete will perform a search to find all items to delete
				deleteOp, pk, err := rh.deleteStatementToDeleteOp(del)
				if err != nil {
					log.Errorf("Node %v CqlRequestError: %v", rh.ID, err)
					errs = append(errs, err.Error())
				} else {
					conflictDomain, err := rh.cdResolver.KeyToConflictDomain(pk, tableUUID)
					conflictDomainB64 := conflictDomain.B64()
					if err != nil {
						log.Errorf("Node %v CqlRequestError: %v", rh.ID, err)
						errs = append(errs, err.Error())
						break
					}
					if slowDeletes[conflictDomainB64] == nil {
						slowDeletes[conflictDomainB64] = make(map[int]operations.DeleteOp, 0)
					}
					slowDeletes[conflictDomainB64][CDOpsCounter[conflictDomainB64]] = *deleteOp
					CDOpsCounter[conflictDomainB64]++
				}
			}
		}
	}

	readInfos := make(map[string]map[int]*readMeta, 0)
	for _, selectQuery := range selects {
		readInfo, err := rh.selectToReadInfo(selectQuery)
		if err != nil {
			errs = append(errs, err.Error())
		} else {
			conflictDomainB64 := readInfo.ConflictDomainKey.B64()
			if readInfos[conflictDomainB64] == nil {
				readInfos[conflictDomainB64] = make(map[int]*readMeta)
			}
			readInfos[conflictDomainB64][CDOpsCounter[conflictDomainB64]] = readInfo
			CDOpsCounter[readInfo.ConflictDomainKey.B64()]++
		}
	}

	if len(errs) > 0 {
		// if we have any errors at this stage, do not go further and report to client
		cqlReply := request.CqlReply{
			ClientID:  m.ClientID,
			RequestId: m.RequestId,
			CqlOuts:   nil,
			Timestamp: m.Timestamp,
			Errs:      errs,
		}
		m.Reply(cqlReply)
		return
	}

	if len(CDOpsCounter) > 0 {
		// do replication requests if we have any ops to replicate
		replyChan := make(chan request.OperationReplicationReply, len(CDOpsCounter))
		reqWrapper := rh.compileReplicationOperations(writeOps, updateOps, slowDeletes, readInfos, CDOpsCounter, &m, replyChan)
		go rh.receiveReplies(ctx, reqWrapper, replyChan)
		for _, replicationReq := range reqWrapper.replicationRequests {
			log.Debugf("Node %v Enqueueing replication request %v in request handler", rh.ID, replicationReq)
			rh.replicationOperationDispatcher.EnqueueOperation(ctx, replicationReq)
		}
	}
}

/*****************************************************************
 * Local methods to support CqlRequestHandler
 ****************************************************************/
func (rh *CqlRequestHandler) compileDDLReplicationOperations(
	createTbls []cql_parser.CreateTableStatement,
	m *request.CqlRequest,
	replyChan chan request.OperationReplicationReply) *requestWrapper {
	reqWrapper := &requestWrapper{
		originalCqlRequest:  *m,
		replicationRequests: make([]request.OperationReplicationRequest, 0, len(createTbls)),
		readMetas:           make(map[ids.CommandID]*readMeta),
		recvdReplies:        0,
		replies:             make([]request.OperationReplicationReply, 0, len(createTbls)),
	}

	for _, ct := range createTbls {
		tblSpec := *cql_store.NewLocusDBTableSpec(uuid.New(), ct.TableName, ct.ColSpecs)
		tblSpec.SkipCreateIfExists = ct.IfNotExist
		replicationRequest := request.OperationReplicationRequest{
			Command: store.Command{
				CommandID:         ids.CommandID(rh.nextCommandID()),
				ConflictDomainKey: conflict_domain.DdlConflictDomain,
				OperationType:     store.DDL_WRITE,
				Operation:         tblSpec,
				TargetStore:       store.CQL,
			},
			Timestamp:  m.Timestamp,
			ClientZone: m.ClientID.ZoneId,
			C:          replyChan,
		}
		reqWrapper.replicationRequests = append(reqWrapper.replicationRequests, replicationRequest)
	}
	return reqWrapper
}

func (rh *CqlRequestHandler) compileReplicationOperations(
	writeOps map[string]map[int]writeop.WriteOp,
	updateOps map[string]map[int]operations.CqlUpdateOp,
	slowDeletes map[string]map[int]operations.DeleteOp,
	readInfos map[string]map[int]*readMeta,
	CDOpCounter map[string]int,
	m *request.CqlRequest,
	replyChan chan request.OperationReplicationReply) *requestWrapper {
	reqWrapper := &requestWrapper{
		originalCqlRequest:  *m,
		replicationRequests: make([]request.OperationReplicationRequest, 0, len(CDOpCounter)),
		readMetas:           make(map[ids.CommandID]*readMeta),
		recvdReplies:        0,
		replies:             make([]request.OperationReplicationReply, 0, len(CDOpCounter)),
	}

	for cdkeyb64, opCount := range CDOpCounter {
		if opCount > 1 {
			mtx := minitx.NewEmptyMiniTx(opCount)
			for opNum := 0; opNum < opCount; opNum++ {
				mtxStep := minitx.MiniTxStep{}
				if writeOp, exists := writeOps[cdkeyb64][opNum]; exists {
					mtxStep.WriteOp = &writeOp
				}
				if readInfo, exists := readInfos[cdkeyb64][opNum]; exists {
					mtxStep.ReadOp = readInfo.readop
				}
				if delOp, exists := slowDeletes[cdkeyb64][opNum]; exists {
					mtxStep.DeleteOp = &delOp
				}
				if updateOp, exists := updateOps[cdkeyb64][opNum]; exists {
					mtxStep.UpdateOp = &updateOp
				}
				mtx.Steps = append(mtx.Steps, mtxStep)
			}

			replicationRequest := request.OperationReplicationRequest{
				Command: store.Command{
					CommandID:         ids.CommandID(rh.nextCommandID()),
					ConflictDomainKey: store.ByteStringFromB64(cdkeyb64),
					OperationType:     store.MINI_TX,
					Operation:         mtx,
					TargetStore:       store.CQL,
				},
				Timestamp:  m.Timestamp,
				ClientZone: m.ClientID.ZoneId,
				C:          replyChan,
			}
			reqWrapper.replicationRequests = append(reqWrapper.replicationRequests, replicationRequest)

		} else {
			// we can take a shortcut and just replicate a single op instead of putting it in the mini tx
			if writeOp, exists := writeOps[cdkeyb64]; exists {
				// we have a write
				replicationRequest := request.OperationReplicationRequest{
					Command: store.Command{
						CommandID:         ids.CommandID(rh.nextCommandID()),
						ConflictDomainKey: store.ByteStringFromB64(cdkeyb64),
						OperationType:     store.WRITE,
						Operation:         writeOp[0],
						TargetStore:       store.CQL,
					},
					Timestamp:  m.Timestamp,
					ClientZone: m.ClientID.ZoneId,
					C:          replyChan,
				}
				reqWrapper.replicationRequests = append(reqWrapper.replicationRequests, replicationRequest)
				continue
			}

			if read, exists := readInfos[cdkeyb64]; exists {
				cmdId := ids.CommandID(rh.nextCommandID())
				replicationRequest := request.OperationReplicationRequest{
					Command: store.Command{
						CommandID:         cmdId,
						ConflictDomainKey: read[0].ConflictDomainKey,
						OperationType:     store.READ,
						Operation:         *read[0].readop,
						TargetStore:       store.CQL,
					},
					Timestamp:  m.Timestamp,
					ClientZone: m.ClientID.ZoneId,
					C:          replyChan,
				}
				reqWrapper.replicationRequests = append(reqWrapper.replicationRequests, replicationRequest)
				reqWrapper.readMetas[cmdId] = read[0]
				continue
			}

			if deleteOps, exists := slowDeletes[cdkeyb64]; exists {
				// we have a single slow delete
				replicationRequest := request.OperationReplicationRequest{
					Command: store.Command{
						CommandID:         ids.CommandID(rh.nextCommandID()),
						ConflictDomainKey: store.ByteStringFromB64(cdkeyb64),
						OperationType:     store.DELETE,
						Operation:         deleteOps[0],
						TargetStore:       store.CQL,
					},
					Timestamp:  m.Timestamp,
					ClientZone: m.ClientID.ZoneId,
					C:          replyChan,
				}
				reqWrapper.replicationRequests = append(reqWrapper.replicationRequests, replicationRequest)
				continue
			}

			if updateOp, exists := updateOps[cdkeyb64]; exists {
				// we have a single slow delete
				replicationRequest := request.OperationReplicationRequest{
					Command: store.Command{
						CommandID:         ids.CommandID(rh.nextCommandID()),
						ConflictDomainKey: store.ByteStringFromB64(cdkeyb64),
						OperationType:     store.UPDATE,
						Operation:         updateOp[0],
						TargetStore:       store.CQL,
					},
					Timestamp:  m.Timestamp,
					ClientZone: m.ClientID.ZoneId,
					C:          replyChan,
				}
				reqWrapper.replicationRequests = append(reqWrapper.replicationRequests, replicationRequest)
				continue
			}
		}
	}
	if len(reqWrapper.replicationRequests) > 0 {
		log.Debugf("Node %v Compiled %d ReplicationOperationRequests (First CommandID=%d) from user request ClientId=%v, ClientCmdId=%d", rh.ID, len(reqWrapper.replicationRequests), reqWrapper.replicationRequests[0].Command.CommandID, reqWrapper.originalCqlRequest.ClientID, reqWrapper.originalCqlRequest.RequestId)
	}
	return reqWrapper
}

/****************************************************************
 * insertToRow produces a row from an insert struct
 ***************************************************************/
func (rh *CqlRequestHandler) insertToRow(insert cql_parser.InsertStatement) (cql_data_model.Row, uuid.UUID, bool, error) {
	tbl := rh.cqlStore.GetTableByName(insert.TableName)
	if tbl == nil {
		return nil, uuid.UUID{}, false, fmt.Errorf("node %v: could not find table %v", rh.ID, insert.TableName)
	}

	hasExpression := false
	row := cql_data_model.Row{}
	for i, colName := range insert.Columns {
		colspec := tbl.GetSchema().GetColumnSpecByName(colName)
		if colspec == nil {
			return nil, uuid.UUID{}, false, fmt.Errorf("column %v is not in the table %v", colName, tbl.GetSchema().GetName())
		}
		colId := colspec.GetID()

		val, err := insert.Values[i].CastIfPossible(colspec.DataType)
		if err != nil {
			log.Errorf("Incorrect data type in column %s: %v", colspec.Name, err)
			return nil, uuid.UUID{}, false, fmt.Errorf("incorrect data type in column %s", colspec.Name)
		}

		row.SetCell(colId, val)
		if insert.Values[i].GetType() == cql_data_model.Expression {
			hasExpression = true
		}
	}
	return row, tbl.GetSchema().GetTableUUID(), hasExpression, nil
}

/****************************************************************
 * updateToRow produces a row from an update struct if all
 * assignments are constant.
 ***************************************************************/
func (rh *CqlRequestHandler) updateToRow(update cql_parser.UpdateStatement) (cql_data_model.Row, uuid.UUID, bool, error) {
	tbl := rh.cqlStore.GetTableByName(update.TableName)
	if tbl == nil {
		return nil, uuid.UUID{}, false, fmt.Errorf("2node %v: could not find table %v", rh.ID, update.TableName)
	}

	needUpdateOp := false
	row := cql_data_model.Row{}
	for _, assignment := range update.Assignments {
		colspec := tbl.GetSchema().GetColumnSpecByName(assignment.Column1Name)
		if colspec == nil {
			return nil, tbl.GetSchema().GetTableUUID(), false, fmt.Errorf("column %v is not in the table %v", assignment.Column1Name, tbl.GetSchema().GetName())
		}
		colId := colspec.GetID()

		if assignment.Value2.GetType() != cql_data_model.Null && assignment.Value2.GetType() != cql_data_model.Expression {
			val, err := assignment.Value2.CastIfPossible(colspec.DataType)
			if err != nil {
				log.Errorf("Incorrect data type in column %s: %v", colspec.Name, err)
				return nil, tbl.GetSchema().GetTableUUID(), false, fmt.Errorf("incorrect data type in column %s", colspec.Name)
			}
			row.SetCell(colId, val)
		} else {
			row.SetCell(colId, assignment.Value2)
		}

		if assignment.Value2.GetType() == cql_data_model.Expression {
			needUpdateOp = true
		}
	}

	// relations should uniquely identify a row in CQL. However, we do that check later. Here we just represent them into a Row
	relationColsSet := make(map[uint8]bool, len(update.Relations))
	for _, relation := range update.Relations {
		colspec := tbl.GetSchema().GetColumnSpecByName(relation.Column1Name)

		val, err := relation.Const2.CastIfPossible(colspec.DataType)
		if err != nil {
			log.Errorf("Incorrect data type in column %s: %v", colspec.Name, err)
			return nil, uuid.UUID{}, false, fmt.Errorf("incorrect data type in column %s", colspec.Name)
		}

		if relation.Relation == cql_data_model.EQUAL {
			row.SetCell(colspec.GetID(), val)
			relationColsSet[colspec.GetID()] = true
		} else {
			return nil, tbl.GetSchema().GetTableUUID(), false, errors.New("only equality relations are supported in UPDATE")
		}
	}

	rowKeyCount := tbl.GetSchema().GetClusteringKeyCount() + tbl.GetSchema().GetPartitionKeyCount()
	if len(relationColsSet) < rowKeyCount {
		needUpdateOp = true
	}

	return row, tbl.GetSchema().GetTableUUID(), needUpdateOp, nil
}

/****************************************************************
 * deleteToRow produces a row from a delete struct.
 * the row essentially has all columns we set to nul, which
 * translate to deletes in the KV layer
 ***************************************************************/
func (rh *CqlRequestHandler) deleteToRow(delete cql_parser.DeleteStatement) (cql_data_model.Row, uuid.UUID, error) {
	tbl := rh.cqlStore.GetTableByName(delete.TableName)
	if tbl == nil {
		return nil, uuid.UUID{}, fmt.Errorf("node %v: could not find table %v", rh.ID, delete.TableName)
	}

	row := cql_data_model.Row{}
	var colNames []string
	if delete.AllColumns {
		colNames = make([]string, len(tbl.GetSchema().ColumnSpecs))
		for i, col := range tbl.GetSchema().ColumnSpecs {
			colNames[i] = col.GetName()
		}
	} else {
		colNames = delete.Columns
	}

	for _, columnName := range colNames {
		assignment := cql_parser.SimpleAssignment{
			Column1Name: columnName,
			Value2:      cql_data_model.NullVal,
		}
		colspec := tbl.GetSchema().GetColumnSpecByName(columnName)
		if colspec == nil {
			return nil, tbl.GetSchema().GetTableUUID(), fmt.Errorf("column %v is not in the table %v", assignment.Column1Name, tbl.GetSchema().GetName())
		}
		colId := colspec.GetID()

		row.SetCell(colId, assignment.Value2)
	}

	// relations should uniquely identify a row in CQL. However, we do that check later. Here we just represent them into a Row
	for _, relation := range delete.Relations {
		colspec := tbl.GetSchema().GetColumnSpecByName(relation.Column1Name)

		val, err := relation.Const2.CastIfPossible(colspec.DataType)
		if err != nil {
			log.Errorf("Incorrect data type in column %s: %v", colspec.Name, err)
			return nil, tbl.GetSchema().GetTableUUID(), fmt.Errorf("incorrect data type in column %s", colspec.Name)
		}

		if relation.Relation == cql_data_model.EQUAL {
			row.SetCell(colspec.GetID(), val)
		} else {
			return nil, tbl.GetSchema().GetTableUUID(), nil // we have no valid row, but it is not an error, we will do slow delete
		}
	}

	return row, tbl.GetSchema().GetTableUUID(), nil
}

/****************************************************************
 * selectToReadInfo produces a list of Partition ConflictDomainKey values to
 * identify the partition and a list of Clustering ConflictDomainKey values and
 * relations on clustering key to select slice of a partition
 * returns readOp, tableUUID, error
 *
 * General flow of reads:
 * (1) Select CQL -> (2) SelectQuery -> (3) PKs, CKs, and Comparator Types ->
 * -> (4) ReadOp ~> (5) Rows ->
 * -> (6) Row with filtered columns -> (7) CqlOutput
 * 1: Client
 * 2: Parser at client or node_assembler
 * 3: request handler: selectToReadInfo
 * 4: request handler using CqlStore (CqlTable)
 * ~replication~
 * 5: store apply produces rows
 * 6: request handler using CqlStore (CqlTable)
 * 7: request handler
 ***************************************************************/
func (rh *CqlRequestHandler) selectToReadInfo(selectQuery cql_parser.SelectQuery) (*readMeta, error) {
	tbl := rh.cqlStore.GetTableByName(selectQuery.TableName)
	if tbl == nil {
		return nil, fmt.Errorf("S node %v: could not find table %v", rh.ID, selectQuery.TableName)
	}

	readOp, pks, nonKeyColumnConditions, err := rh.constructReadOp(tbl, selectQuery.Relations)
	if err != nil {
		return nil, err
	}

	var colIds []uint8
	var colTypes []cql_data_model.LocusDBType
	var colNamesOrAliases []string
	if selectQuery.AllColumns {
		colIds = make([]uint8, len(tbl.GetSchema().ColumnSpecs))
		colTypes = make([]cql_data_model.LocusDBType, len(tbl.GetSchema().ColumnSpecs))
		colNamesOrAliases = make([]string, len(tbl.GetSchema().ColumnSpecs))
		for i, col := range tbl.GetSchema().ColumnSpecs {
			colNamesOrAliases[i] = col.GetName()
			colIds[i] = col.GetID()
			colTypes[i] = col.GetDataType()
		}
	} else {
		colIds = make([]uint8, len(selectQuery.Columns))
		colTypes = make([]cql_data_model.LocusDBType, len(selectQuery.Columns))
		colNamesOrAliases = make([]string, len(selectQuery.Columns))
		for i, col := range selectQuery.Columns {
			colSpec := tbl.GetSchema().GetColumnSpecByName(col)
			if colSpec == nil {
				return nil, fmt.Errorf("column %v is not in the table %v", col, tbl.GetSchema().GetName())
			}
			colIds[i] = colSpec.GetID()
			colTypes[i] = colSpec.GetDataType()
			if alias, exists := selectQuery.Aliases[i]; exists {
				colNamesOrAliases[i] = alias
			} else {
				colNamesOrAliases[i] = col
			}
		}
	}

	conflictDomain, err := rh.cdResolver.KeyToConflictDomain(tbl.PartitionKeyFromValues(pks), readOp.TableUUID)
	if err != nil {
		return nil, err
	}
	readInfo := &readMeta{
		namesOrAliases: colNamesOrAliases,
		colTypes:       colTypes,
		readop: &readop.CQLReadOp{
			ReadOp:                 readOp,
			ColIds:                 colIds,
			NonKeyColumnConditions: nonKeyColumnConditions,
		},
		ConflictDomainKey: conflictDomain,
	}

	return readInfo, nil
}

func (rh *CqlRequestHandler) deleteStatementToDeleteOp(delete cql_parser.DeleteStatement) (*operations.DeleteOp, store.ByteString, error) {
	tbl := rh.cqlStore.GetTableByName(delete.TableName)
	if tbl == nil {
		return nil, nil, fmt.Errorf("D node %v: could not find table %v", rh.ID, delete.TableName)
	}

	readOp, pks, nonKeyColumnConditions, err := rh.constructReadOp(tbl, delete.Relations)
	if err != nil {
		return nil, nil, err
	}

	var colNames []string
	if delete.AllColumns {
		colNames = make([]string, len(tbl.GetSchema().ColumnSpecs))
		for i, col := range tbl.GetSchema().ColumnSpecs {
			colNames[i] = col.GetName()
		}
	} else {
		colNames = make([]string, len(delete.Columns))
		for i, col := range delete.Columns {
			colSpec := tbl.GetSchema().GetColumnSpecByName(col)
			if colSpec == nil {
				return nil, nil, fmt.Errorf("column %v is not in the table %v", col, tbl.GetSchema().GetName())
			}
			colNames[i] = col
		}
	}

	deleteOp := &operations.DeleteOp{
		ReadSpec:               *readOp,
		ColumnNames:            colNames,
		NonKeyColumnConditions: nonKeyColumnConditions,
	}

	conflictDomain, err := rh.cdResolver.KeyToConflictDomain(tbl.PartitionKeyFromValues(pks), readOp.TableUUID)
	if err != nil {
		return nil, nil, err
	}
	return deleteOp, conflictDomain, nil
}

func (rh *CqlRequestHandler) constructReadOp(tbl *cql_store.CQLTable, relations []*cql_parser.SimpleRelation) (*readop.ReadOp, []cql_data_model.LocusDBValue, map[uint8]*cql_data_model.ConstRelation, error) {
	pks := make([]cql_data_model.LocusDBValue, tbl.GetSchema().GetPartitionKeyCount())
	numPks := 0
	cksTemp := make([]cql_data_model.LocusDBValue, tbl.GetSchema().GetClusteringKeyCount())
	ckRelationsTemp := make([]cql_data_model.ComparatorType, tbl.GetSchema().GetClusteringKeyCount())
	nonKeyColumnRelations := make(map[uint8]*cql_data_model.ConstRelation)
	numCks := 0
	for _, rel := range relations {
		colspec := tbl.GetSchema().GetColumnSpecByName(rel.Column1Name)
		if colspec == nil {
			return nil, nil, nil, fmt.Errorf("column %v is not in the table %v", rel.Column1Name, tbl.GetSchema().GetName())
		}
		colNumber := colspec.GetOrderNum()
		colVal, err := rel.Const2.CastIfPossible(colspec.DataType)
		if err != nil {
			log.Errorf("Incorrect data type in column comparison: %v", err)
			return nil, nil, nil, fmt.Errorf("incorrect data type in column comparison: %v", err)
		}

		if colspec.GetKind() == cql_store.PartitionColumn {
			pks[colNumber] = colVal
			numPks += 1
		} else if colspec.GetKind() == cql_store.ClusteringColumn {
			cksTemp[int(colNumber)-tbl.GetSchema().GetPartitionKeyCount()] = colVal
			ckRelationsTemp[int(colNumber)-tbl.GetSchema().GetPartitionKeyCount()] = rel.Relation
			numCks += 1
		} else {
			// regular column in WHERE clause? Original cassandra does not support this without an index
			// for us, we need to have a partition to search in non-key columns within the partition
			nonKeyColumnRelations[colNumber] = cql_data_model.NewConstRelation(colNumber, rel.Relation, colVal)
		}
	}

	cks := make([]cql_data_model.LocusDBValue, numCks)
	ckRelations := make([]cql_data_model.ComparatorType, numCks)
	if numCks > 0 {
		j := 0
		for i, ck := range cksTemp {
			if ck == nil {
				if j+1 < numCks {
					// we can not have a nil in between two non-nil cks.
					// for example, if we have 3 cks: 1,2,3, we cannot have a query with where clause specifying cks 1 and 3
					// or 2 and 3, or just 2 or 3
					return nil, nil, nil, errors.New("missing clustering key in where clause")
				}
			} else {
				cks[j] = ck
				ckRelations[j] = ckRelationsTemp[i]
				j += 1
			}
		}
	}

	if numPks != tbl.GetSchema().GetPartitionKeyCount() {
		return nil, nil, nil, fmt.Errorf("must provide all components of a partition key, only %d out of %d were given", len(pks), tbl.GetSchema().GetPartitionKeyCount())
	}

	var readOp *readop.ReadOp

	if numCks == 0 {
		readOp = tbl.ReadPartition(pks)
	} else {
		readOp = tbl.ReadPartitionSlice(pks, cks, ckRelations)
	}

	return readOp, pks, nonKeyColumnRelations, nil
}

func (rh *CqlRequestHandler) replyToClient(reqWrapper *requestWrapper) {
	cqlReply := new(request.CqlReply)
	origReq := reqWrapper.originalCqlRequest
	cqlReply.ClientID = origReq.ClientID
	cqlReply.Timestamp = origReq.Timestamp
	errs := make([]string, 0)

	for _, replyOut := range reqWrapper.replies {
		if replyOut.Err != nil {
			errs = append(errs, replyOut.Err.Error())
			continue
		}

		if (replyOut.OperationType == store.READ || replyOut.OperationType == store.MINI_TX) && len(replyOut.CmdResults) > 0 {
			rows := replyOut.CmdResults[0].Result.(*cql_store.TableSnippet) // result from cql store is a list of rows
			// TODO: investigate if it is worth filtering out items from the full row
			// for now just slap the columns IDs expected from the query to the output for the client
			rm := reqWrapper.readMetas[replyOut.CommandID]
			if rm == nil {
				log.Errorf("Node %v could not find read meta in requestWrapper %+v. ReplyOut=%+v", rh.ID, reqWrapper.readMetas, replyOut)
				return
			}
			tblId := rm.readop.TableUUID
			cqlOut := request.CqlOutput{
				TableUUID: tblId,
				// TODO: should we make table name easier to access by copying it somewhere in reqWrapper or readInfo?
				// GetTable acquires a read lock on tables map in store, this should not be too bad, but maybe worth a look at some time
				TableName:    rh.cqlStore.GetTable(tblId).GetSchema().GetName(),
				Rows:         rows.Rows,
				DisplayOrder: rm.readop.ColIds,
				ColumnNames:  rm.namesOrAliases,
				ColumnTypes:  rm.colTypes,
				LeaderID:     replyOut.Ballot.ID(),
			}

			cqlReply.CqlOuts = append(cqlReply.CqlOuts, cqlOut)
		} else if replyOut.OperationType == store.WRITE || replyOut.OperationType == store.DELETE {
			// we still want cql out of ok for write
			if replyOut.Err != nil && len(replyOut.CmdResults) == 1 {
				tblId := replyOut.CmdResults[0].TableId
				cqlOut := request.NewOkRowCqlOut(tblId, rh.cqlStore.GetTable(tblId).GetSchema().GetName(), replyOut.Ballot.ID())
				cqlReply.CqlOuts = append(cqlReply.CqlOuts, cqlOut)
			}
		}
	}

	cqlReply.Errs = errs
	cqlReply.RequestId = origReq.RequestId
	origReq.Reply(*cqlReply)
}

func (rh *CqlRequestHandler) nextCommandID() uint64 {
	return atomic.AddUint64(&rh.lastCommandID, 1)
}

func (rh *CqlRequestHandler) replyError(origReq request.CqlRequest, err error) {
	log.Debugf("Replying to client_driver with error: %v", err)
	cqlReply := new(request.CqlReply)
	cqlReply.Errs = []string{err.Error()}
	origReq.Reply(*cqlReply)
}

/*******************************************************************
 * Receives the reply from the store when the replication finishes
 * and command applies.
 *******************************************************************/
func (rh *CqlRequestHandler) receiveReplies(ctx context.Context, reqWrapper *requestWrapper, replyChan chan request.OperationReplicationReply) {
	// here we are waiting for all replies
	ctx, cancel := context.WithTimeout(ctx, time.Duration(rh.cfg.RequestTimeoutMs)*time.Millisecond)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			// this is likely a timeout from one or more underlying layers.
			// This may include a replication layer and its phase-1 (leader election) and phase-2 (replication)
			// operations.
			// This may also be the timeout at the store of this node after the replication is successful.
			// Note that the timeout does not mean the operation did not or will not take an effect. Once the operation
			// hit the execution queue, it will be executed at some time and in the correct order regardless if the
			// request timed out or not. Similar with replication -- the operation will be retried until successful
			// or cancelled. cancelled replication remains in the log of this node and may be brought up to life
			// upon recovery.
			// Regardless we still want to return to client with an error, otherwise we will hit the timeout at the
			// client
			log.Errorf("Timeout waiting for client request %v operations on node %v", reqWrapper.originalCqlRequest, rh.ID)
			if reqWrapper.recvdReplies == 0 {
				rh.replyError(reqWrapper.originalCqlRequest, ctx.Err())
			} else {
				// we had some replication requests completed, so return partial result
				errReply := request.OperationReplicationReply{
					Err: ctx.Err(),
				}
				reqWrapper.replies = append(reqWrapper.replies, errReply)
				rh.replyToClient(reqWrapper)
			}
			return
		case replicationreply := <-replyChan:
			// check if we have readresults
			log.Debugf("received replication reply: %v", replicationreply)
			reqWrapper.recvdReplies += 1
			reqWrapper.replies = append(reqWrapper.replies, replicationreply)
			if reqWrapper.recvdReplies == len(reqWrapper.replicationRequests) {
				rh.replyToClient(reqWrapper)
				return
			}
		}
	}
}
