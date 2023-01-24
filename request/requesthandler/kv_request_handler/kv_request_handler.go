package kv_request_handler

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/UNH-DistSyS/UNH-CLT/conflict_domain"
	"github.com/UNH-DistSyS/UNH-CLT/core"
	"github.com/UNH-DistSyS/UNH-CLT/database"
	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/log"
	"github.com/UNH-DistSyS/UNH-CLT/replication/operations/readop"
	"github.com/UNH-DistSyS/UNH-CLT/replication/operations/writeop"
	"github.com/UNH-DistSyS/UNH-CLT/request"
	"github.com/UNH-DistSyS/UNH-CLT/store"
	"github.com/google/uuid"
)

type KVRequestHandler struct {
	ids.ID                         // must have an identity
	tableIdResolver                database.TableIdResolver
	replicationOperationDispatcher core.OperationDispatcher
	lastCommandID                  uint64
	cdResolver                     conflict_domain.ConflictDomainResolver
	canServe                       bool
}

func NewKVRequestHandler(identity ids.ID,
	replicationOperationDispatcher core.OperationDispatcher,
	tableIdResolver database.TableIdResolver,
	cdResolver conflict_domain.ConflictDomainResolver) *KVRequestHandler {
	requestHandler := new(KVRequestHandler)
	requestHandler.ID = identity
	requestHandler.replicationOperationDispatcher = replicationOperationDispatcher
	requestHandler.tableIdResolver = tableIdResolver
	requestHandler.lastCommandID = uint64(ids.NewCommandId(identity, 0))
	requestHandler.canServe = false
	requestHandler.cdResolver = cdResolver
	log.Infof("Creating KV Client Request Handler on node id = %v \n", requestHandler.ID)

	return requestHandler
}

/*************************************
	Handles
 ************************************/

// HandleClientRequest registers with httpserv or netman to receive requests from clients
// this is where we do he translation between client_driver request and replication requests
// client_driver request is translated into either a read, write or DDL write replication operation
func (rh *KVRequestHandler) HandleClientRequest(ctx context.Context, req interface{}) {
	m := req.(request.KVRequest)
	if !rh.canServe || rh.cdResolver == nil {
		rh.replyError(m, errors.New("KV server is not yet ready to serve client requests"))
		return
	}
	log.Debugf("KVRequestHandler %s received %v\n", rh.ID, m)
	tableId := rh.tableIdResolver.ResolveTableId(m.Command.Table)

	if tableId == nil && m.Command.OperationType != store.DDL_WRITE {
		// cannot do anything with a nil table unless it is creating such table
		rh.replyError(m, errors.New(fmt.Sprintf("could not find table %s", m.Command.Table)))
		return
	}

	var op interface{}
	switch m.Command.OperationType {
	case store.READ:
		op = readop.ReadOp{
			TableUUID: *tableId,
			ReadMode:  readop.POINT,
			SkipFirst: false,
			SkipLast:  false,
			StartKey:  m.Command.Key,
		}
	case store.WRITE:
		op = writeop.WriteOp{
			TableUUID: *tableId,
			Items:     []store.KVItem{{Key: m.Command.Key, Value: m.Command.Value}},
		}
	case store.DDL_WRITE:
		if tableId != nil {
			rh.replyError(m, errors.New(fmt.Sprintf("table %s already exists", m.Command.Table)))
			return
		}
		tableUUID := uuid.New()
		tableId = &tableUUID
		op = store.KVTableSpec{
			TableUUID: tableUUID,
			TableName: m.Command.Table,
		}
	}

	// start building replication request
	conflictDomainKey, err := rh.cdResolver.KeyToConflictDomain(m.Command.Key, *tableId)
	if err != nil {
		rh.replyError(m, errors.New(fmt.Sprintf("incorrect conflict domain: %s", err.Error())))
		return
	}

	replyChan := make(chan request.OperationReplicationReply, 1)
	replicationReq := new(request.OperationReplicationRequest)
	replicationReq.Timestamp = m.Timestamp
	replicationReq.C = replyChan
	replicationReq.Command = store.Command{
		CommandID:         ids.CommandID(rh.nextCommandID()),
		ConflictDomainKey: conflictDomainKey,
		OperationType:     m.Command.OperationType,
		TargetStore:       store.KV,
	}

	replicationReq.Command.Operation = op
	log.Debugf("Enqueuing replication operation %v", replicationReq)
	go rh.receiveReplies(m, replyChan)
	rh.replicationOperationDispatcher.EnqueueOperation(ctx, *replicationReq)
}

func (rh *KVRequestHandler) Start() {
	rh.canServe = true
}

/*****************************************************************
 * Local methods to support KVRequestHandler
 ****************************************************************/

func (rh *KVRequestHandler) replyToClient(origReq request.KVRequest, replicationReply *request.OperationReplicationReply) {
	log.Debugf("Replying to client_driver on replicationReply %v", replicationReply)
	if replicationReply.Err != nil {
		rh.replyError(origReq, replicationReply.Err)
		return
	}
	kvReply := new(request.KVReply)
	kvReply.Command = origReq.Command
	kvReply.Timestamp = replicationReply.Timestamp
	if origReq.Command.OperationType == store.READ && len(replicationReply.CmdResults) == 1 {
		kvItems := replicationReply.CmdResults[0].Result.([]store.KVItem)
		if len(kvItems) > 0 {
			kvReply.Value = kvItems[0].Value
		}
	} else if origReq.Command.OperationType == store.MINI_TX {
		// TODO: KV transactions
	}
	origReq.Reply(*kvReply)
}

func (rh *KVRequestHandler) replyError(origReq request.KVRequest, err error) {
	log.Debugf("Replying to client_driver with error: %v", err)
	kvReply := new(request.KVReply)
	kvReply.Err = err.Error()
	kvReply.Command = origReq.Command
	origReq.Reply(*kvReply)
}

func (rh *KVRequestHandler) nextCommandID() uint64 {
	return atomic.AddUint64(&rh.lastCommandID, 1)
}

func (rh *KVRequestHandler) receiveReplies(origReq request.KVRequest, replyChan chan request.OperationReplicationReply) {
	replicationreply := <-replyChan
	rh.replyToClient(origReq, &replicationreply)
}
