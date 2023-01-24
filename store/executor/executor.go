package executor

import (
	"sync"

	"github.com/UNH-DistSyS/UNH-CLT/config"
	"github.com/UNH-DistSyS/UNH-CLT/database"
	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/log"
	"github.com/UNH-DistSyS/UNH-CLT/operation_dispatcher"
	"github.com/UNH-DistSyS/UNH-CLT/request"
	"github.com/UNH-DistSyS/UNH-CLT/store"
	"github.com/UNH-DistSyS/UNH-CLT/wal"
)

type OperationExecutor struct {
	ids.ID // we must have an identity
	store  database.Store
	config *config.Config
	operation_dispatcher.ConflictSequentialOperationDispatcher

	sync.RWMutex
}

func NewOperationExecutor(id ids.ID, cfg *config.Config, store database.Store) *OperationExecutor {
	e := new(OperationExecutor)
	e.ID = id // identity
	e.config = cfg
	e.store = store
	e.ConflictSequentialOperationDispatcher = *operation_dispatcher.NewConflictSequentialOperationDispatcher(e.ID, cfg.ChanBufferSize, cfg.OpDispatchConcurrency, "ConflictDomainKeyB64")
	e.Register(&wal.LockingLogEntry{}, e.ExecuteCommand)
	e.Run()
	return e
}

func (ex *OperationExecutor) ExecuteCommand(e *wal.LockingLogEntry) {
	e.Lock()
	defer e.Unlock()
	e.Command.Version = e.Slot
	canReplyOnLogEntry := e.CanReply()
	log.Debugf("executor %s applying command %v\n", ex.ID, e.Command)
	vals, err := ex.store.ApplyCommand(&e.Command, !canReplyOnLogEntry)
	if err == nil {
		log.Debugf("Node %v executed with no errors", ex.ID)
		if canReplyOnLogEntry {
			reply := request.OperationReplicationReply{
				CommandID:         e.Command.CommandID,
				ConflictDomainKey: e.Command.ConflictDomainKey,
				OperationType:     e.Command.OperationType,
				Ballot:            e.Ballot,
			}
			if vals != nil {
				reply.CmdResults = append(reply.CmdResults, *vals)
			}
			log.Debugf("Node %v Replying for commandId=%v on slot=%d", ex.ID, e.Command.CommandID, e.Slot)
			e.Reply(reply)
			e.SetReplyChan(nil)
		}
	} else {
		if err != store.ErrNotFound {
			log.Errorf("Node %v ExecuteCommand Error {entry = %v}: %s\n", ex.ID, e, err)
		}
		if canReplyOnLogEntry {
			log.Debugf("reply with err (key=%s): %s\n", e.Command.ConflictDomainKey, err)
			e.Reply(request.OperationReplicationReply{
				CommandID:         e.Command.CommandID,
				ConflictDomainKey: e.Command.ConflictDomainKey,
				OperationType:     e.Command.OperationType,
				Err:               err,
			})
			e.SetReplyChan(nil)
		}
	}
	e.TransitionToState(wal.StateExecuted)
}
