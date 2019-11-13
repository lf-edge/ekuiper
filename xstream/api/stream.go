package api

import (
	"context"
	"github.com/sirupsen/logrus"
)

type ConsumeFunc func(data interface{})

type Closable interface {
	Close(StreamContext) error
}

type Source interface {
	//Should be sync function for normal case. The container will run it in go func
	Open(StreamContext, ConsumeFunc) error
	Closable
}

type Sink interface {
	//Should be sync function for normal case. The container will run it in go func
	Open(StreamContext) error
	Collect(StreamContext, interface{}) error
	Closable
}

type Emitter interface {
	AddOutput(chan<- interface{}, string) error
}

type Collector interface {
	GetInput() (chan<- interface{}, string)
}

type TopNode interface {
	GetName() string
}

type Rule struct {
	Id      string                   `json:"id"`
	Sql     string                   `json:"sql"`
	Actions []map[string]interface{} `json:"actions"`
	Options map[string]interface{}   `json:"options"`
}

type StreamContext interface {
	context.Context
	GetLogger()  *logrus.Entry
	GetRuleId() string
	GetOpId() string
	WithMeta(ruleId string, opId string) StreamContext
	WithCancel() (StreamContext, context.CancelFunc)
}

type Operator interface {
	Emitter
	Collector
	Exec(StreamContext, chan<- error)
	GetName() string
}

