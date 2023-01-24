package operation_dispatcher

import (
	"reflect"
	"sync"

	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/log"
)

type ConflictSequentialOperationDispatcher struct {
	ids.ID                  // must have an identity
	operationQueue          chan interface{}
	conflictPropertyName    string
	handles                 map[string]reflect.Value
	closeChan               chan bool
	conflictBuckets         map[int]chan interface{}
	bucketCloseChan         map[int]chan bool
	conflictBucketsLock     sync.RWMutex
	maxConcurrentOperations int
	//utils.Semaphore
}

func NewConflictSequentialOperationDispatcher(identity ids.ID, chanBufferSize, maxConcurrentOperations int, conflictPropertyName string) *ConflictSequentialOperationDispatcher {
	log.Infof("Creating ConflictSequentialOperationDispatcher at node %v with max concurrency=%d", identity, maxConcurrentOperations)
	od := new(ConflictSequentialOperationDispatcher)
	od.ID = identity
	if maxConcurrentOperations <= 0 {
		log.Fatalf("Max Concurrency for operation dispatcher must be greater than 0")
	}
	od.conflictPropertyName = conflictPropertyName
	od.maxConcurrentOperations = maxConcurrentOperations
	od.operationQueue = make(chan interface{}, chanBufferSize)
	od.handles = make(map[string]reflect.Value)
	od.closeChan = make(chan bool, 1)
	od.bucketCloseChan = make(map[int]chan bool, maxConcurrentOperations)
	od.conflictBuckets = make(map[int]chan interface{}, maxConcurrentOperations)
	for i := 0; i < maxConcurrentOperations; i++ {
		bucket := make(chan interface{}, chanBufferSize)
		bc := make(chan bool, 1)
		//bucketLock <- struct{}{}
		od.conflictBuckets[i] = bucket
		od.bucketCloseChan[i] = bc
	}
	return od
}

// Register a handle function for each operation type handled by the queue. This function is not thread safe -- make sure all messages are registered before running
func (od *ConflictSequentialOperationDispatcher) Register(m interface{}, f interface{}) {
	t := reflect.TypeOf(m)
	log.Infof("Node %v Registering %s on the ConflictSequentialOperationDispatcher ", od.ID, t.String())
	fn := reflect.ValueOf(f)
	if fn.Kind() != reflect.Func || fn.Type().NumIn() != 1 || (fn.Type().In(0) != t && fn.Type().In(1).Kind() != reflect.Interface) {
		panic("register handle function error")
	}

	reflectedM := reflect.ValueOf(m)
	if reflectedM.Kind() == reflect.Struct && reflectedM.FieldByName(od.conflictPropertyName) == (reflect.Value{}) {
		panic("register handle function error: conflict property is not in struct")
	}

	if reflectedM.Kind() == reflect.Ptr && reflectedM.Elem().FieldByName(od.conflictPropertyName) == (reflect.Value{}) {
		panic("register handle function error: conflict property is not in struct")
	}

	if _, exists := od.handles[t.String()]; exists {
		log.Warningf("Handler for %s already registered on node %v. Existing handler is overwritten", t.String(), od.ID)
	}
	od.handles[t.String()] = fn
}

// Run start and run the node
func (od *ConflictSequentialOperationDispatcher) Run() {
	log.Infof("Starting ConflictSequentialOperationDispatcher at node %v", od.ID)
	if len(od.handles) > 0 {
		for bucketNum, _ := range od.conflictBuckets {
			go od.handleBucket(bucketNum)
		}
		go od.handle()
	} else {
		log.Errorf("ConflictSequentialOperationDispatcher has no handles to run on node %v", od.ID)
	}
}

// Close stops the dispatcher
func (od *ConflictSequentialOperationDispatcher) Close() {
	log.Infof("Stopping ConflictSequentialOperationDispatcher at node %v\n", od.ID)
	od.closeChan <- false
}

func (od *ConflictSequentialOperationDispatcher) handleBucket(bucketNum int) {
	for {
		select {
		case op := <-od.conflictBuckets[bucketNum]:
			if op != nil {
				v := reflect.ValueOf(op)
				name := v.Type().String()
				f, exists := od.handles[name]
				if !exists {
					log.Errorf("ConflictSequentialOperationDispatcher. No registered handle function for operation type %v on node %v", name, od.ID)
					continue
				}

				f.Call([]reflect.Value{v})

			} else {
				log.Infof("Operation dispatcher dequeued a nil operation on ode %v", od.ID)
			}
		}
	}
}

func (od *ConflictSequentialOperationDispatcher) handle() {
	for {
		select {
		case <-od.closeChan:
			log.Infof("Node %v, ConflictSequentialOperationDispatcher closing", od.ID)
			for bucketNum, _ := range od.conflictBuckets {
				od.bucketCloseChan[bucketNum] <- true
			}
			return
		case op := <-od.operationQueue:
			if op != nil {
				v := reflect.ValueOf(op)
				var conflict reflect.Value
				switch v.Kind() {
				case reflect.Struct:
					conflict = v.FieldByName(od.conflictPropertyName)
				case reflect.Ptr:
					conflict = v.Elem().FieldByName(od.conflictPropertyName)
				}
				conflictBucketNum := od.conflictKeyToBucket(conflict.String())
				conflictBucket := od.conflictBuckets[conflictBucketNum]
				conflictBucket <- op
			} else {
				log.Infof("Operation dispatcher dequeued a nil operation on ode %v", od.ID)
			}
		}
	}
}

func (od *ConflictSequentialOperationDispatcher) conflictKeyToBucket(conflictkey string) int {
	// this is stupid
	if conflictkey == "" {
		return 0
	}

	b := 0
	for i := 0; i < len(conflictkey); i++ {
		b += int(conflictkey[i])
	}
	return b % od.maxConcurrentOperations
}

func (od *ConflictSequentialOperationDispatcher) EnqueueOperation(op interface{}) {
	od.operationQueue <- op
}
