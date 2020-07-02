package checkpoints

import (
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/api"
)

type StreamTask interface {
	Broadcast(data interface{}) error
	GetName() string
	GetStreamContext() api.StreamContext
}

type NonSourceTask interface {
	StreamTask
	GetInputCount() int
	AddInputCount()

	InitCheckpoint(BarrierHandler, xsql.Qos)
}

type BufferOrEvent struct {
	Data      interface{}
	Channel   string
	Processed bool
}

type Message int

const (
	STOP Message = iota
	ACK
	DEC
)

type Signal struct {
	Message Message
	Barrier
}

type Barrier struct {
	CheckpointId int64
	OpId         string
}
