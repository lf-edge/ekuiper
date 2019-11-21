package api

import (
	"context"
	"engine/xsql"
	"github.com/sirupsen/logrus"
)

//The function to call when data is emitted by the source.
type ConsumeFunc func(message xsql.Message, metadata xsql.Metadata)

type Closable interface {
	Close(ctx StreamContext) error
}

type Source interface {
	//Should be sync function for normal case. The container will run it in go func
	Open(ctx StreamContext, consume ConsumeFunc) error
	//Called during initialization. Configure the source with the data source(e.g. topic for mqtt) and the properties
	//read from the yaml
	Configure(datasource string, props map[string]interface{}) error
	Closable
}

type Sink interface {
	//Should be sync function for normal case. The container will run it in go func
	Open(ctx StreamContext) error
	//Called during initialization. Configure the sink with the properties from rule action definition
	Configure(props map[string]interface{}) error
	//Called when each row of data has transferred to this sink
	Collect(ctx StreamContext, data interface{}) error
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

