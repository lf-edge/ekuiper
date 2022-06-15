// Copyright 2022 EMQ Technologies Co., Ltd.
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
const ExceptionsTotal = "exceptions_total"
const ProcessLatencyUs = "process_latency_us"
const LastInvocation = "last_invocation"
const BufferLength = "buffer_length"

var MetricNames = []string{RecordsInTotal, RecordsOutTotal, ExceptionsTotal, ProcessLatencyUs, BufferLength, LastInvocation}

type StatManager interface {
	IncTotalRecordsIn()
	IncTotalRecordsOut()
	IncTotalExceptions()
	ProcessTimeStart()
	ProcessTimeEnd()
	SetBufferLength(l int64)
	GetMetrics() []interface{}
}

//The statManager is not thread safe. Make sure it is used in only one instance
type DefaultStatManager struct {
	//metrics
	totalRecordsIn  int64
	totalRecordsOut int64
	totalExceptions int64
	processLatency  int64
	lastInvocation  time.Time
	bufferLength    int64
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

func (sm *DefaultStatManager) IncTotalExceptions() {
	sm.totalExceptions++
	var t time.Time
	sm.processTimeStart = t
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

func (sm *DefaultStatManager) GetMetrics() []interface{} {
	result := []interface{}{
		sm.totalRecordsIn, sm.totalRecordsOut, sm.totalExceptions, sm.processLatency, sm.bufferLength,
	}

	if !sm.lastInvocation.IsZero() {
		result = append(result, sm.lastInvocation.Format("2006-01-02T15:04:05.999999"))
	} else {
		result = append(result, 0)
	}

	return result
}
