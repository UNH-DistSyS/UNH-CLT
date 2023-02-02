package operation_dispatcher

import "context"

// dispatcherOperationContainer is used for internal queuing and buffering before the operation and its context
// is moved to an appropriate handler
type dispatcherOperationContainer struct {
	ctx context.Context
	op  interface{}
}
