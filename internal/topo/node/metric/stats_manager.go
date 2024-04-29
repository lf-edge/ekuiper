// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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
	"time"

	"github.com/lf-edge/ekuiper/pkg/api"
)

const (
	RecordsInTotal         = "records_in_total"
	RecordsOutTotal        = "records_out_total"
	MessagesProcessedTotal = "messages_processed_total"
	ProcessLatencyUs       = "process_latency_us"
	ProcessLatencyUsHist   = "process_latency_us_hist"
	LastInvocation         = "last_invocation"
	BufferLength           = "buffer_length"
	ExceptionsTotal        = "exceptions_total"
	LastException          = "last_exception"
	LastExceptionTime      = "last_exception_time"
)

var MetricNames = []string{RecordsInTotal, RecordsOutTotal, MessagesProcessedTotal, ProcessLatencyUs, BufferLength, LastInvocation, ExceptionsTotal, LastException, LastExceptionTime}

type StatManager interface {
	IncTotalRecordsIn()
	IncTotalRecordsOut()
	IncTotalMessagesProcessed(n int64)
	IncTotalExceptions(err string)
	ProcessTimeStart()
	ProcessTimeEnd()
	SetBufferLength(l int64)
	SetProcessTimeStart(t time.Time)
	GetMetrics() []any
	// Clean remove all metrics history
	Clean(ruleId string)
}

// DefaultStatManager The statManager is not thread safe. Make sure it is used in only one instance
type DefaultStatManager struct {
	// metrics
	totalRecordsIn         int64
	totalRecordsOut        int64
	totalMessagesProcessed int64

	processLatency    int64
	lastInvocation    time.Time
	bufferLength      int64
	totalExceptions   int64
	lastException     string
	lastExceptionTime time.Time
	// configs
	opType           string //"source", "op", "sink"
	prefix           string
	processTimeStart time.Time
	opId             string
	instanceId       int
}

func NewStatManager(ctx api.StreamContext, opType string) StatManager {
	var prefix string
	switch opType {
	case "source":
		prefix = "source_"
	case "op":
		prefix = "op_"
	case "sink":
		prefix = "sink_"
	}
	ds := DefaultStatManager{
		opType:     opType,
		prefix:     prefix,
		opId:       ctx.GetOpId(),
		instanceId: ctx.GetInstanceId(),
	}
	sm, err := getStatManager(ctx, ds)
	if err != nil {
		ctx.GetLogger().Warnf("Fail to create extra stat manager for %s %s: %v", opType, ctx.GetOpId(), err)
	}
	return sm
}

func (sm *DefaultStatManager) IncTotalRecordsIn() {
	sm.totalRecordsIn++
}

func (sm *DefaultStatManager) IncTotalMessagesProcessed(n int64) {
	sm.totalMessagesProcessed += n
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

func (sm *DefaultStatManager) GetMetrics() []any {
	result := []interface{}{
		sm.totalRecordsIn,
		sm.totalRecordsOut,
		sm.totalMessagesProcessed,
		sm.processLatency,
		sm.bufferLength,
		int64(0),
		sm.totalExceptions,
		sm.lastException,
		int64(0),
	}

	if !sm.lastInvocation.IsZero() {
		result[5] = sm.lastInvocation.UnixMilli()
	}
	if !sm.lastExceptionTime.IsZero() {
		result[8] = sm.lastExceptionTime.UnixMilli()
	}
	return result
}

func (sm *DefaultStatManager) Clean(_ string) {
	// do nothing
}
