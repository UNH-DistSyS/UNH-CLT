package executor

import (
	"github.com/UNH-DistSyS/UNH-CLT/config"
	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/log"
	"github.com/UNH-DistSyS/UNH-CLT/operation_dispatcher"
	"github.com/UNH-DistSyS/UNH-CLT/request"
	"github.com/UNH-DistSyS/UNH-CLT/wal"
)

type MockExecutor struct {
	operation_dispatcher.ContextlessSequentialOperationDispatcher
}

func NewMockExecutor(id ids.ID, cfg *config.Config) *MockExecutor {
	e := new(MockExecutor)
	e.ContextlessSequentialOperationDispatcher = *operation_dispatcher.NewContextlessSequentialOperationDispatcher(id, cfg.ChanBufferSize)
	e.Register(&wal.LockingLogEntry{}, e.ExecuteCommand)
	return e
}

func (m *MockExecutor) ExecuteCommand(e *wal.LockingLogEntry) {
	// mark executed
	e.Lock()
	log.Debugf("Node locking slot=%d", e.Slot)
	defer e.Unlock()
	log.Debugf("MockExecutor %v executing log entry %v", m.ID, e)
	e.TransitionToState(wal.StateExecuted)
	e.Reply(request.OperationReplicationReply{
		CommandID:         e.Command.CommandID,
		ConflictDomainKey: e.Command.ConflictDomainKey,
		OperationType:     e.Command.OperationType,
		Ballot:            e.Ballot,
		CmdResults:        nil,
		Timestamp:         0,
		Err:               nil,
	})
}
