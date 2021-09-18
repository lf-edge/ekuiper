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

type Source interface {
	// Open Should be sync function for normal case. The container will run it in go func
	Open(ctx StreamContext, consumer chan<- SourceTuple, errCh chan<- error)
	// Configure Called during initialization. Configure the source with the data source(e.g. topic for mqtt) and the properties read from the yaml
	Configure(datasource string, props map[string]interface{}) error
	Closable
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

type Sink interface {
	//Should be sync function for normal case. The container will run it in go func
	Open(ctx StreamContext) error
	//Called during initialization. Configure the sink with the properties from rule action definition
	Configure(props map[string]interface{}) error
	//Called when each row of data has transferred to this sink
	Collect(ctx StreamContext, data interface{}) error
	Closable
}

type Closable interface {
	Close(ctx StreamContext) error
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

type StreamContext interface {
	context.Context
	GetLogger() Logger
	GetRuleId() string
	GetOpId() string
	GetInstanceId() int
	WithMeta(ruleId string, opId string) StreamContext
	WithInstance(instanceId int) StreamContext
	WithCancel() (StreamContext, context.CancelFunc)
}

type FunctionContext interface {
	StreamContext
	GetFuncId() int
}
