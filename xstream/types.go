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
	Open(context.Context, chan<- error)
}

type Operator interface{
	Emitter
	Collector
	Exec(context context.Context) error
}

type TopNode interface{
	GetName() string
}

type Rule struct{
	Id string `json:"id"`
	Sql string `json:"sql"`
	Actions []map[string]interface{} `json:"actions"`
	Options map[string]interface{} `json:"options"`
}