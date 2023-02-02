package core

import (
	"context"

	"github.com/UNH-DistSyS/UNH-CLT/ids"
)

type Runnable interface {
	Run()
	Close()
}

type MessageRegistrable interface {
	Register(m interface{}, f interface{})
}

type OperationDispatcher interface {
	MessageRegistrable
	Runnable
	EnqueueOperation(ctx context.Context, op interface{})
}

type ContextlessOperationDispatcher interface {
	MessageRegistrable
	Runnable
	EnqueueOperation(op interface{})
}

type ContextMeta struct {
	RequestID ids.CommandID

	CurrentMessageSender  ids.ID // the sender of message/request the node is handling
	CurrentMessageCycleId uint64
}

type AliveStatus uint8

const (
	AliveStatusUp AliveStatus = iota
	AliveStatusDown
	AliveStatusRecovering
)

type HealthState struct {
	Heartbeat            int
	HeartbeatReceiveTime int64
	AliveStatus          AliveStatus
	CpuUtil              uint8
	MemUtil              uint8
	StorageUtil          uint8
}
