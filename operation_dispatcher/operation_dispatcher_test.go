package operation_dispatcher

import (
	"context"
	"flag"
	"math/rand"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/stretchr/testify/assert"
)

type TestOp struct {
	I int
}

type TestOpConflict struct {
	I  int
	CD string
}

var opsEnqueued int32
var opsHandled int32
var ctxValOk bool

func handleTestOp(ctx context.Context, op TestOp) {
	val := ctx.Value("testkey")
	if val != "testval" {
		ctxValOk = false
	}
	atomic.AddInt32(&opsHandled, 1)
}

func handleTestOpConflict(op TestOpConflict) {
	atomic.AddInt32(&opsHandled, 1)
}

func TestSequentialOperationDispatcher(t *testing.T) {
	ctxValOk = true
	flag.Parse()
	od := NewSequentialOperationDispatcher(*ids.GetIDFromString("1.1"), 100)
	od.Register(TestOp{}, handleTestOp)
	od.Run()
	for i := 1; i <= 20; i++ {
		op := TestOp{I: i}
		opsEnqueued++
		ctx := context.WithValue(context.Background(), "testkey", "testval")
		od.EnqueueOperation(ctx, op)
	}

	// sleep 100 ms to let ops drain
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, opsEnqueued, atomic.LoadInt32(&opsHandled), "Expected %d, got %d", opsEnqueued, opsHandled)
	assert.True(t, ctxValOk)
}

func TestConcurrentOperationDispatcher(t *testing.T) {
	flag.Parse()
	od := NewConcurrentOperationDispatcher(*ids.GetIDFromString("1.1"), 100, 2)
	od.Register(TestOp{}, handleTestOp)
	od.Run()
	for i := 1; i <= 20; i++ {
		op := TestOp{I: i}
		opsEnqueued++
		ctx := context.WithValue(context.Background(), "testkey", "testval")
		od.EnqueueOperation(ctx, op)
	}

	// sleep 100 ms to let ops drain
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, opsEnqueued, atomic.LoadInt32(&opsHandled), "Expected %d, got %d", opsEnqueued, opsHandled)
}

func TestConflictOperationDispatcher(t *testing.T) {
	ctxValOk = true
	flag.Parse()
	od := NewConflictSequentialOperationDispatcher(*ids.GetIDFromString("1.1"), 100, 20, "CD")
	od.Register(TestOpConflict{}, handleTestOpConflict)
	od.Run()
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 1; i <= 20000; i++ {
		op := TestOpConflict{I: i, CD: "cd1" + strconv.Itoa(int(rnd.Int31n(20)))}
		opsEnqueued++
		od.EnqueueOperation(op)
	}

	// sleep 100 ms to let ops drain
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, opsEnqueued, atomic.LoadInt32(&opsHandled), "Expected %d, got %d", opsEnqueued, opsHandled)
	assert.True(t, ctxValOk)
}
