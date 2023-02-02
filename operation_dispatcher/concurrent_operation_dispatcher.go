package operation_dispatcher

import (
	"context"
	"reflect"

	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/log"
	"github.com/UNH-DistSyS/UNH-CLT/utils"
)

type ConcurrentOperationDispatcher struct {
	ids.ID         // must have an identity
	operationQueue chan dispatcherOperationContainer
	handles        map[string]reflect.Value
	closeChan      chan bool
	utils.Semaphore
}

func NewConcurrentOperationDispatcher(identity ids.ID, chanBufferSize, maxConcurrentOperations int) *ConcurrentOperationDispatcher {
	log.Infof("Creating ConcurrentOperationDispatcher at node %v with max concurrency=%d", identity, maxConcurrentOperations)
	od := new(ConcurrentOperationDispatcher)
	od.ID = identity
	if maxConcurrentOperations <= 0 {
		log.Fatalf("Max Concurrency for operation dispatcher must be greater than 0")
	}
	od.Semaphore = utils.NewSemaphore(maxConcurrentOperations)
	od.operationQueue = make(chan dispatcherOperationContainer, chanBufferSize)
	od.handles = make(map[string]reflect.Value)
	od.closeChan = make(chan bool, 1)
	return od
}

// Register a handle function for each operation type handled by the queue. This function is not thread safe -- make sure all messages are registered before running
func (od *ConcurrentOperationDispatcher) Register(m interface{}, f interface{}) {
	t := reflect.TypeOf(m)
	log.Infof("Node %v Registering %s on the ConcurrentOperationDispatcher ", od.ID, t.String())
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

// Run start and run the node
func (od *ConcurrentOperationDispatcher) Run() {
	log.Infof("Starting ConcurrentOperationDispatcher at node %v", od.ID)
	if len(od.handles) > 0 {
		go od.handle()
	} else {
		log.Errorf("ConcurrentOperationDispatcher has no handles to run on node %v", od.ID)
	}
}

// Close stops the dispatcher
func (od *ConcurrentOperationDispatcher) Close() {
	log.Infof("Stopping SequentialOperationDispatcher at node %v\n", od.ID)
	od.closeChan <- false
}

func (od *ConcurrentOperationDispatcher) handle() {
	for {
		select {
		case <-od.closeChan:
			log.Infof("Node %v, ConcurrentOperationDispatcher closing", od.ID)
			return
		case opContainer := <-od.operationQueue:
			op := opContainer.op
			if op != nil {
				v := reflect.ValueOf(op)
				name := v.Type().String()
				f, exists := od.handles[name]
				if !exists {
					log.Errorf("ConcurrentOperationDispatcher. No registered handle function for operation type %v on node %v", name, od.ID)
					continue
				}
				od.Acquire(1) // get one more semaphore
				go func() {

					f.Call([]reflect.Value{reflect.ValueOf(opContainer.ctx), v})
					od.Release(1)
				}()
			} else {
				log.Infof("Operation dispatcher dequeued a nil operation on ode %v", od.ID)
			}
		}
	}
}

func (od *ConcurrentOperationDispatcher) EnqueueOperation(ctx context.Context, op interface{}) {
	od.operationQueue <- dispatcherOperationContainer{ctx, op}
}
