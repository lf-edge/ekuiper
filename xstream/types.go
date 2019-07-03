package xstream

import (
	"context"
)

type Emitter interface {
	AddOutput(chan<- interface{}, string)
}

type Source interface {
	Emitter
	Open(context context.Context) error
}

type Collector interface {
	GetInput() (chan<- interface{}, string)
}

type Sink interface {
	Collector
	Open(context context.Context) <-chan error
}

type Operator interface{
	Emitter
	Collector
	Exec(context context.Context) error
}

type TopNode interface{
	GetName() string
}