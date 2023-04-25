// Copyright 2022-2023 EMQ Technologies Co., Ltd.
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

package metric

import (
	"fmt"
	"github.com/lf-edge/ekuiper/pkg/api"
	"time"
)

const RecordsInTotal = "records_in_total"
const RecordsOutTotal = "records_out_total"
const ProcessLatencyUs = "process_latency_us"
const LastInvocation = "last_invocation"
const BufferLength = "buffer_length"
const ExceptionsTotal = "exceptions_total"
const LastException = "last_exception"
const LastExceptionTime = "last_exception_time"

var MetricNames = []string{RecordsInTotal, RecordsOutTotal, ProcessLatencyUs, BufferLength, LastInvocation, ExceptionsTotal, LastException, LastExceptionTime}

type StatManager interface {
	IncTotalRecordsIn()
	IncTotalRecordsOut()
	IncTotalExceptions(err string)
	ProcessTimeStart()
	ProcessTimeEnd()
	SetBufferLength(l int64)
	SetProcessTimeStart(t time.Time)
	GetMetrics() []interface{}
	// Clean remove all metrics history
	Clean(ruleId string)
}

// DefaultStatManager The statManager is not thread safe. Make sure it is used in only one instance
type DefaultStatManager struct {
	//metrics
	totalRecordsIn    int64
	totalRecordsOut   int64
	processLatency    int64
	lastInvocation    time.Time
	bufferLength      int64
	totalExceptions   int64
	lastException     string
	lastExceptionTime time.Time
	//configs
	opType           string //"source", "op", "sink"
	prefix           string
	processTimeStart time.Time
	opId             string
	instanceId       int
}

func NewStatManager(ctx api.StreamContext, opType string) (StatManager, error) {
	var prefix string
	switch opType {
	case "source":
		prefix = "source_"
	case "op":
		prefix = "op_"
	case "sink":
		prefix = "sink_"
	default:
		return nil, fmt.Errorf("invalid opType %s, must be \"source\", \"sink\" or \"op\"", opType)
	}

	ds := DefaultStatManager{
		opType:     opType,
		prefix:     prefix,
		opId:       ctx.GetOpId(),
		instanceId: ctx.GetInstanceId(),
	}
	return getStatManager(ctx, ds)
}

func (sm *DefaultStatManager) IncTotalRecordsIn() {
	sm.totalRecordsIn++
}

func (sm *DefaultStatManager) IncTotalRecordsOut() {
	sm.totalRecordsOut++
}

func (sm *DefaultStatManager) IncTotalExceptions(err string) {
	sm.totalExceptions++
	var t time.Time
	sm.processTimeStart = t
	sm.lastException = err
	sm.lastExceptionTime = time.Now()
}

func (sm *DefaultStatManager) ProcessTimeStart() {
	sm.lastInvocation = time.Now()
	sm.processTimeStart = sm.lastInvocation
}

func (sm *DefaultStatManager) ProcessTimeEnd() {
	if !sm.processTimeStart.IsZero() {
		sm.processLatency = int64(time.Since(sm.processTimeStart) / time.Microsecond)
	}
}

func (sm *DefaultStatManager) SetBufferLength(l int64) {
	sm.bufferLength = l
}

func (sm *DefaultStatManager) SetProcessTimeStart(t time.Time) {
	sm.processTimeStart = t
	sm.lastInvocation = t
}

func (sm *DefaultStatManager) GetMetrics() []interface{} {
	result := []interface{}{
		sm.totalRecordsIn,
		sm.totalRecordsOut,
		sm.processLatency,
		sm.bufferLength,
		0,
		sm.totalExceptions,
		sm.lastException,
		0,
	}

	if !sm.lastInvocation.IsZero() {
		result[4] = sm.lastInvocation.Format("2006-01-02T15:04:05.999999")
	}
	if !sm.lastExceptionTime.IsZero() {
		result[7] = sm.lastExceptionTime.Format("2006-01-02T15:04:05.999999")
	}
	return result
}

func (sm *DefaultStatManager) Clean(_ string) {
	// do nothing
}
