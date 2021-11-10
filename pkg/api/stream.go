// Copyright 2021 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"context"
	"sync"
)

type SourceTuple interface {
	Message() map[string]interface{}
	Meta() map[string]interface{}
}

type DefaultSourceTuple struct {
	Mess map[string]interface{} `json:"message"`
	M    map[string]interface{} `json:"meta"`
}

func NewDefaultSourceTuple(message map[string]interface{}, meta map[string]interface{}) *DefaultSourceTuple {
	return &DefaultSourceTuple{
		Mess: message,
		M:    meta,
	}
}

func (t *DefaultSourceTuple) Message() map[string]interface{} {
	return t.Mess
}
func (t *DefaultSourceTuple) Meta() map[string]interface{} {
	return t.M
}

type Logger interface {
	Debug(args ...interface{})
	Info(args ...interface{})
	Warn(args ...interface{})
	Error(args ...interface{})
	Debugln(args ...interface{})
	Infoln(args ...interface{})
	Warnln(args ...interface{})
	Errorln(args ...interface{})
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

type Store interface {
	SaveState(checkpointId int64, opId string, state map[string]interface{}) error
	SaveCheckpoint(checkpointId int64) error //Save the whole checkpoint state into storage
	GetOpState(opId string) (*sync.Map, error)
	Clean() error
}

type Closable interface {
	Close(ctx StreamContext) error
}

type Source interface {
	//Should be sync function for normal case. The container will run it in go func
	Open(ctx StreamContext, consumer chan<- SourceTuple, errCh chan<- error)
	//Called during initialization. Configure the source with the data source(e.g. topic for mqtt) and the properties
	//read from the yaml
	Configure(datasource string, props map[string]interface{}) error
	Closable
}

type TableSource interface {
	// Load the data at batch
	Load(ctx StreamContext) ([]SourceTuple, error)
	//Called during initialization. Configure the source with the data source(e.g. topic for mqtt) and the properties
	//read from the yaml
	Configure(datasource string, props map[string]interface{}) error
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

type Rewindable interface {
	GetOffset() (interface{}, error)
	Rewind(offset interface{}) error
}

type RuleOption struct {
	IsEventTime        bool  `json:"isEventTime" yaml:"isEventTime"`
	LateTol            int64 `json:"lateTolerance" yaml:"lateTolerance"`
	Concurrency        int   `json:"concurrency" yaml:"concurrency"`
	BufferLength       int   `json:"bufferLength" yaml:"bufferLength"`
	SendMetaToSink     bool  `json:"sendMetaToSink" yaml:"sendMetaToSink"`
	SendError          bool  `json:"sendError" yaml:"sendError"`
	Qos                Qos   `json:"qos" yaml:"qos"`
	CheckpointInterval int   `json:"checkpointInterval" yaml:"checkpointInterval"`
}

type Rule struct {
	Triggered bool                     `json:"triggered"`
	Id        string                   `json:"id"`
	Sql       string                   `json:"sql"`
	Actions   []map[string]interface{} `json:"actions"`
	Options   *RuleOption              `json:"options"`
}

type StreamContext interface {
	context.Context
	GetLogger() Logger
	GetRuleId() string
	GetOpId() string
	GetInstanceId() int
	GetRootPath() string
	WithMeta(ruleId string, opId string, store Store) StreamContext
	WithInstance(instanceId int) StreamContext
	WithCancel() (StreamContext, context.CancelFunc)
	SetError(e error)
	// State handling
	IncrCounter(key string, amount int) error
	GetCounter(key string) (int, error)
	PutState(key string, value interface{}) error
	GetState(key string) (interface{}, error)
	DeleteState(key string) error
	// Connection related methods
	GetConnection(connectSelector string) (interface{}, error)
	ReleaseConnection(connectSelector string)
	// Properties processing, prop is a json path
	ParseDynamicProp(prop string, data interface{}) (interface{}, error)
	// Transform output according to the properties like syntax
	TransformOutput() ([]byte, bool, error)
}

type Operator interface {
	Emitter
	Collector
	Exec(StreamContext, chan<- error)
	GetName() string
	GetMetrics() [][]interface{}
}

type FunctionContext interface {
	StreamContext
	GetFuncId() int
}

type Function interface {
	//The argument is a list of xsql.Expr
	Validate(args []interface{}) error
	//Execute the function, return the result and if execution is successful.
	//If execution fails, return the error and false.
	Exec(args []interface{}, ctx FunctionContext) (interface{}, bool)
	//If this function is an aggregate function. Each parameter of an aggregate function will be a slice
	IsAggregate() bool
}

const (
	AtMostOnce Qos = iota
	AtLeastOnce
	ExactlyOnce
)

type Qos int
