package operation_dispatcher

import (
	"reflect"

	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/log"
)

type ContextlessSequentialOperationDispatcher struct {
	ids.ID         // must have an identity
	operationQueue chan interface{}
	handles        map[string]reflect.Value
	closeChan      chan bool
}

func NewContextlessSequentialOperationDispatcher(identity ids.ID, chanBufferSize int) *ContextlessSequentialOperationDispatcher {
	log.Infof("Creating ContextlessSequentialOperationDispatcher at node %v \n", identity)
	od := new(ContextlessSequentialOperationDispatcher)
	od.ID = identity
	od.operationQueue = make(chan interface{}, chanBufferSize)
	od.handles = make(map[string]reflect.Value)
	return od
}

// Register a handle function for each operation type handled by the queue
func (od *ContextlessSequentialOperationDispatcher) Register(m interface{}, f interface{}) {
	t := reflect.TypeOf(m)
	log.Infof("Node %v Registering %s on the ContextlessSequentialOperationDispatcher ", od.ID, t.String())
	fn := reflect.ValueOf(f)
	if fn.Kind() != reflect.Func || fn.Type().NumIn() != 1 || (fn.Type().In(0) != t && fn.Type().In(1).Kind() != reflect.Interface) {
		panic("register handle function error")
	}
	if _, exists := od.handles[t.String()]; exists {
		log.Warningf("Handler for %s already registered on node %v. Existing handler is overwritten", t.String(), od.ID)
	}
	od.handles[t.String()] = fn
}

// Run starts the dispatcher
func (od *ContextlessSequentialOperationDispatcher) Run() {
	log.Infof("Starting SequentialOperationDispatcher at node %v\n", od.ID)
	if len(od.handles) > 0 {
		od.closeChan = make(chan bool, 1)
		go od.handle()
	}
}

// Close stops the dispatcher
func (od *ContextlessSequentialOperationDispatcher) Close() {
	log.Infof("Stopping SequentialOperationDispatcher at node %v\n", od.ID)
	od.closeChan <- false
}

func (od *ContextlessSequentialOperationDispatcher) handle() {
	for {
		select {
		case <-od.closeChan:
			return
		case op := <-od.operationQueue:
			v := reflect.ValueOf(op)
			name := v.Type().String()
			f, exists := od.handles[name]
			if !exists {
				log.Errorf("SequentialOperationDispatcher. No registered handle function for operation type %v", name)
				continue
			}
			f.Call([]reflect.Value{v})
		}
	}
}

func (od *ContextlessSequentialOperationDispatcher) EnqueueOperation(op interface{}) {
	od.operationQueue <- op
}
