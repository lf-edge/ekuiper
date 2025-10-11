// Copyright 2024 EMQ Technologies Co., Ltd.
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

type Logger interface {
	Debug(args ...interface{})
	Info(args ...interface{})
	Warn(args ...interface{})
	Error(args ...interface{})
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

type StreamContext interface {
	context.Context
	GetLogger() Logger
	GetRuleId() string
	GetOpId() string
	GetInstanceId() int
	GetRunId() int
	GetRootPath() string

	WithMeta(ruleId string, opId string, store Store) StreamContext
	WithInstance(instanceId int) StreamContext
	WithRun(runId int) StreamContext
	WithCancel() (StreamContext, context.CancelFunc)
	EnableTracer(enabled bool)
	IsTraceEnabled() bool

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
}
