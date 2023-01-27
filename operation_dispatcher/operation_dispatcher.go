package operation_dispatcher

import (
	"context"
	"reflect"

	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/log"
)

type SequentialOperationDispatcher struct {
	ids.ID         // must have an identity
	operationQueue chan dispatcherOperationContainer
	handles        map[string]reflect.Value
	closeChan      chan bool
}

func NewSequentialOperationDispatcher(identity ids.ID, chanBufferSize int) *SequentialOperationDispatcher {
	log.Infof("Creating SequentialOperationDispatcher at node %v \n", identity)
	od := new(SequentialOperationDispatcher)
	od.ID = identity
	od.operationQueue = make(chan dispatcherOperationContainer, chanBufferSize)
	od.handles = make(map[string]reflect.Value)
	od.closeChan = make(chan bool, 1)
	return od
}

// Register a handle function for each operation type handled by the queue
func (od *SequentialOperationDispatcher) Register(m interface{}, f interface{}) {
	log.Infof("Registering %v on the SequentialOperationDispatcher ", m)
	t := reflect.TypeOf(m)
	fn := reflect.ValueOf(f)
	if fn.Kind() != reflect.Func || fn.Type().NumIn() != 2 || fn.Type().In(0).String() != "context.Context" ||
		(fn.Type().In(1) != t && fn.Type().In(1).Kind() != reflect.Interface) {
		panic("register handle function error")
	}
	if _, exists := od.handles[t.String()]; exists {
		log.Warningf("Handler for %s already registered on node %v. Existing handler is overwritten", t.String(), od.ID)
	}
	od.handles[t.String()] = fn
}

// Run starts the dispatcher
func (od *SequentialOperationDispatcher) Run() {
	log.Infof("Starting SequentialOperationDispatcher at node %v\n", od.ID)
	if len(od.handles) > 0 {
		go od.handle()
	}
}

// Close stops the dispatcher
func (od *SequentialOperationDispatcher) Close() {
	log.Infof("Stopping SequentialOperationDispatcher at node %v\n", od.ID)
	od.closeChan <- false
}

func (od *SequentialOperationDispatcher) handle() {
	for {
		select {
		case <-od.closeChan:
			return
		case opContainer := <-od.operationQueue:
			ctx := opContainer.ctx
			op := opContainer.op
			v := reflect.ValueOf(op)
			name := v.Type().String()
			f, exists := od.handles[name]
			if !exists {
				log.Errorf("SequentialOperationDispatcher. No registered handle function for operation type %v", name)
				continue
			}
			f.Call([]reflect.Value{reflect.ValueOf(ctx), v})
		}
	}
}

func (od *SequentialOperationDispatcher) EnqueueOperation(ctx context.Context, op interface{}) {
	od.operationQueue <- dispatcherOperationContainer{ctx, op}
}
