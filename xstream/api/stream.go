package api

import (
	"context"
	"github.com/sirupsen/logrus"
)

type ConsumeFunc func(data interface{})

type Source interface {
	Open(context StreamContext, consume ConsumeFunc) error
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
	GetContext() context.Context
	GetLogger()  *logrus.Entry
	GetRuleId() string
	GetOpId() string
}

type SinkConnector interface {
	Open(context.Context, chan<- error)
}

type Sink interface {
	Collector
	SinkConnector
}

type Operator interface {
	Emitter
	Collector
	Exec(context context.Context) error
}

