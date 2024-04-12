// Copyright 2021-2024 EMQ Technologies Co., Ltd.
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
	"time"
)

type SourceTuple interface {
	Message() map[string]interface{}
	Meta() map[string]interface{}
	Timestamp() time.Time
}

type RawTuple interface {
	Raw() []byte
}

type DefaultSourceTuple struct {
	Mess map[string]interface{} `json:"message"`
	M    map[string]interface{} `json:"meta"`
	Time time.Time              `json:"timestamp"`
	raw  []byte
}

// NewDefaultRawTuple creates a new DefaultSourceTuple with raw data. Use this when extend source connector
func NewDefaultRawTuple(raw []byte, meta map[string]interface{}, ts time.Time) *DefaultSourceTuple {
	return &DefaultSourceTuple{
		M:    meta,
		Time: ts,
		raw:  raw,
	}
}

// NewDefaultSourceTuple creates a new DefaultSourceTuple with message and metadata. Use this when extend all in one source.
func NewDefaultSourceTuple(message map[string]interface{}, meta map[string]interface{}) *DefaultSourceTuple {
	return &DefaultSourceTuple{
		Mess: message,
		M:    meta,
		Time: time.Now(),
	}
}

func NewDefaultSourceTupleWithTime(message map[string]interface{}, meta map[string]interface{}, timestamp time.Time) *DefaultSourceTuple {
	return &DefaultSourceTuple{
		Mess: message,
		M:    meta,
		Time: timestamp,
	}
}

func (t *DefaultSourceTuple) Message() map[string]interface{} {
	return t.Mess
}

func (t *DefaultSourceTuple) Meta() map[string]interface{} {
	return t.M
}

func (t *DefaultSourceTuple) Timestamp() time.Time {
	return t.Time
}

func (t *DefaultSourceTuple) Raw() []byte {
	return t.raw
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
	// SaveCheckpoint saves the whole checkpoint state into storage
	SaveCheckpoint(checkpointId int64) error
	GetOpState(opId string) (*sync.Map, error)
	Clean() error
}

type Closable interface {
	Close(ctx StreamContext) error
}

type Source interface {
	// Open Should be sync function for normal case. The container will run it in go func
	Open(ctx StreamContext, consumer chan<- SourceTuple, errCh chan<- error)
	// Configure Called during initialization. Configure the source with the data source(e.g. topic for mqtt) and the properties
	// read from the yaml
	Configure(datasource string, props map[string]interface{}) error
	Closable
}

type SourceConnector interface {
	Source
	Connect(ctx StreamContext) error
	Subscriber
}

type Subscriber interface {
	Subscribe(ctx StreamContext) error
}

type LookupSource interface {
	// Open creates the connection to the external data source
	Open(ctx StreamContext) error
	// Configure Called during initialization. Configure the source with the data source(e.g. topic for mqtt) and the properties
	// read from the yaml
	Configure(datasource string, props map[string]interface{}) error
	// Lookup receive lookup values to construct the query and return query results
	Lookup(ctx StreamContext, fields []string, keys []string, values []interface{}) ([]SourceTuple, error)
	Closable
}

type Sink interface {
	// Open Should be sync function for normal case. The container will run it in go func
	Open(ctx StreamContext) error
	// Configure Called during initialization. Configure the sink with the properties from rule action definition
	Configure(props map[string]interface{}) error
	// Collect Called when each row of data has transferred to this sink
	Collect(ctx StreamContext, data interface{}) error
	Closable
}

type ResendSink interface {
	Sink
	// CollectResend Called when the sink cache resend is triggered
	CollectResend(ctx StreamContext, data interface{}) error
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
	ResetOffset(input map[string]interface{}) error
}

type RuleOption struct {
	Debug              bool             `json:"debug" yaml:"debug"`
	LogFilename        string           `json:"logFilename" yaml:"logFilename"`
	IsEventTime        bool             `json:"isEventTime" yaml:"isEventTime"`
	LateTol            int64            `json:"lateTolerance" yaml:"lateTolerance"`
	Concurrency        int              `json:"concurrency" yaml:"concurrency"`
	BufferLength       int              `json:"bufferLength" yaml:"bufferLength"`
	SendMetaToSink     bool             `json:"sendMetaToSink" yaml:"sendMetaToSink"`
	SendError          bool             `json:"sendError" yaml:"sendError"`
	Qos                Qos              `json:"qos" yaml:"qos"`
	CheckpointInterval int              `json:"checkpointInterval" yaml:"checkpointInterval"`
	Restart            *RestartStrategy `json:"restartStrategy" yaml:"restartStrategy"`
	Cron               string           `json:"cron" yaml:"cron"`
	Duration           string           `json:"duration" yaml:"duration"`
	CronDatetimeRange  []DatetimeRange  `json:"cronDatetimeRange" yaml:"cronDatetimeRange"`
}

type DatetimeRange struct {
	Begin          string `json:"begin" yaml:"begin"`
	End            string `json:"end" yaml:"end"`
	BeginTimestamp int64  `json:"beginTimestamp"`
	EndTimestamp   int64  `json:"endTimestamp"`
}

type RestartStrategy struct {
	Attempts     int     `json:"attempts" yaml:"attempts"`
	Delay        int     `json:"delay" yaml:"delay"`
	Multiplier   float64 `json:"multiplier" yaml:"multiplier"`
	MaxDelay     int     `json:"maxDelay" yaml:"maxDelay"`
	JitterFactor float64 `json:"jitter" yaml:"jitter"`
}

type PrintableTopo struct {
	Sources []string                 `json:"sources"`
	Edges   map[string][]interface{} `json:"edges"`
}

type GraphNode struct {
	Type     string                 `json:"type"`
	NodeType string                 `json:"nodeType"`
	Props    map[string]interface{} `json:"props"`
	// UI is a placeholder for ui properties
	UI map[string]interface{} `json:"ui"`
}

// SourceMeta is the meta data of a source node. It describes what existed stream/table to refer to.
// It is part of the Props in the GraphNode and it is optional
type SourceMeta struct {
	SourceName string `json:"sourceName"` // the name of the stream or table
	SourceType string `json:"sourceType"` // stream or table
}

type RuleGraph struct {
	Nodes map[string]*GraphNode `json:"nodes"`
	Topo  *PrintableTopo        `json:"topo"`
}

// Rule the definition of the business logic
// Sql and Graph are mutually exclusive, at least one of them should be set
type Rule struct {
	Triggered bool                     `json:"triggered"`
	Id        string                   `json:"id,omitempty"`
	Name      string                   `json:"name,omitempty"` // The display name of a rule
	Sql       string                   `json:"sql,omitempty"`
	Graph     *RuleGraph               `json:"graph,omitempty"`
	Actions   []map[string]interface{} `json:"actions,omitempty"`
	Options   *RuleOption              `json:"options,omitempty"`
}

func (r *Rule) IsLongRunningScheduleRule() bool {
	if r.Options == nil {
		return false
	}
	return len(r.Options.Cron) == 0 && len(r.Options.Duration) == 0 && len(r.Options.CronDatetimeRange) > 0
}

func (r *Rule) IsScheduleRule() bool {
	if r.Options == nil {
		return false
	}
	return len(r.Options.Cron) > 0 && len(r.Options.Duration) > 0
}

func GetDefaultRule(name, sql string) *Rule {
	return &Rule{
		Id:  name,
		Sql: sql,
		Options: &RuleOption{
			IsEventTime:        false,
			LateTol:            1000,
			Concurrency:        1,
			BufferLength:       1024,
			SendMetaToSink:     false,
			SendError:          true,
			Qos:                AtMostOnce,
			CheckpointInterval: 300000,
			Restart: &RestartStrategy{
				Attempts:     0,
				Delay:        1000,
				Multiplier:   2,
				MaxDelay:     30000,
				JitterFactor: 0.1,
			},
		},
	}
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
	// IncrCounter State handling
	IncrCounter(key string, amount int) error
	GetCounter(key string) (int, error)
	PutState(key string, value interface{}) error
	GetState(key string) (interface{}, error)
	DeleteState(key string) error
	// ParseTemplate parse the template string with the given data
	ParseTemplate(template string, data interface{}) (string, error)
	// ParseJsonPath parse the jsonPath string with the given data
	ParseJsonPath(jsonPath string, data interface{}) (interface{}, error)
	// TransformOutput Transform output according to the properties including dataTemplate, sendSingle, fields
	// TransformOutput first transform data through the dataTemplate property，and then select data based on the fields property
	// It is recommended that you do not configure both the dataTemplate property and the fields property.
	// The second parameter is whether the data is transformed or just return as its json format.
	TransformOutput(data interface{}) ([]byte, bool, error)
	// Decode is set in the source according to the format.
	// It decodes byte array into map or map slice.
	Decode(data []byte) (map[string]interface{}, error)

	DecodeIntoList(data []byte) ([]map[string]interface{}, error)
}

type Operator interface {
	Emitter
	Collector
	Exec(StreamContext, chan<- error)
	GetName() string
	GetMetrics() []any
}

type FunctionContext interface {
	StreamContext
	GetFuncId() int
}

type Function interface {
	// Validate The argument is a list of xsql.Expr
	Validate(args []interface{}) error
	// Exec Execute the function, return the result and if execution is successful.
	// If execution fails, return the error and false.
	Exec(args []interface{}, ctx FunctionContext) (interface{}, bool)
	// IsAggregate If this function is an aggregate function. Each parameter of an aggregate function will be a slice
	IsAggregate() bool
}

const (
	AtMostOnce Qos = iota
	AtLeastOnce
	ExactlyOnce
)

type Qos int

type MessageClient interface {
	Subscribe(c StreamContext, subChan []TopicChannel, messageErrors chan error, params map[string]interface{}) error
	Publish(c StreamContext, topic string, message []byte, params map[string]interface{}) error
	Ping() error
}

// TopicChannel is the data structure for subscriber
type TopicChannel struct {
	// Topic for subscriber to filter on if any
	Topic string
	// Messages is the returned message channel for the subscriber
	Messages chan<- interface{}
}
