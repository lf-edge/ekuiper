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

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/syncx"
)

const (
	RecordsInTotal                    = "records_in_total"
	RecordsOutTotal                   = "records_out_total"
	MessagesProcessedTotal            = "messages_processed_total"
	ProcessLatencyUs                  = "process_latency_us"
	ProcessLatencyUsHist              = "process_latency_us_hist"
	LastInvocation                    = "last_invocation"
	BufferLength                      = "buffer_length"
	ExceptionsTotal                   = "exceptions_total"
	LastException                     = "last_exception"
	LastExceptionTime                 = "last_exception_time"
	ConnectionStatus                  = "connection_status"
	ConnectionLastConnectedTime       = "connection_last_connected_time"
	ConnectionLastDisconnectedTime    = "connection_last_disconnected_time"
	ConnectionLastDisconnectedMessage = "connection_last_disconnected_message"
	ConnectionLastTryTime             = "connection_last_try_time"
)

var MetricNames = []string{RecordsInTotal, RecordsOutTotal, MessagesProcessedTotal, ProcessLatencyUs, BufferLength, LastInvocation, ExceptionsTotal, LastException, LastExceptionTime, ConnectionStatus, ConnectionLastConnectedTime, ConnectionLastDisconnectedTime, ConnectionLastDisconnectedMessage, ConnectionLastTryTime}

type StatManager interface {
	IncTotalRecordsIn()
	IncTotalRecordsOut()
	IncTotalMessagesProcessed(n int64)
	IncTotalExceptions(err string)
	ProcessTimeStart()
	ProcessTimeEnd()
	SetBufferLength(l int64)
	SetProcessTimeStart(t time.Time)
	// 0 is connecting, 1 is connected, -1 is disconnected
	SetConnectionState(state string, message string)
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

	connectionState *ConnectionStatManager
	// configs
	opType           string //"source", "op", "sink"
	prefix           string
	processTimeStart time.Time
	opId             string
	instanceId       int
	syncx.RWMutex
}

func NewStatManager(ctx api.StreamContext, opType string) StatManager {
	var ds *DefaultStatManager
	switch opType {
	case "source":
		ds = &DefaultStatManager{
			opType:          opType,
			prefix:          "source_",
			opId:            ctx.GetOpId(),
			instanceId:      ctx.GetInstanceId(),
			connectionState: &ConnectionStatManager{},
		}
	case "op":
		ds = &DefaultStatManager{
			opType:     opType,
			prefix:     "op_",
			opId:       ctx.GetOpId(),
			instanceId: ctx.GetInstanceId(),
		}
	case "sink":
		ds = &DefaultStatManager{
			opType:          opType,
			prefix:          "sink_",
			opId:            ctx.GetOpId(),
			instanceId:      ctx.GetInstanceId(),
			connectionState: &ConnectionStatManager{},
		}
	}
	sm, err := getStatManager(ctx, ds)
	if err != nil {
		ctx.GetLogger().Warnf("Fail to create extra stat manager for %s %s: %v", opType, ctx.GetOpId(), err)
	}
	return sm
}

func (sm *DefaultStatManager) SetConnectionState(status string, message string) {
	sm.Lock()
	defer sm.Unlock()
	if sm.connectionState != nil {
		sm.connectionState.SetConnectionState(status, message)
	}
}

func (sm *DefaultStatManager) IncTotalRecordsIn() {
	sm.Lock()
	defer sm.Unlock()
	sm.totalRecordsIn++
}

func (sm *DefaultStatManager) IncTotalMessagesProcessed(n int64) {
	sm.Lock()
	defer sm.Unlock()
	sm.totalMessagesProcessed += n
}

func (sm *DefaultStatManager) IncTotalRecordsOut() {
	sm.Lock()
	defer sm.Unlock()
	sm.totalRecordsOut++
}

func (sm *DefaultStatManager) IncTotalExceptions(err string) {
	sm.Lock()
	defer sm.Unlock()
	sm.incTotalExceptions(err)
}

func (sm *DefaultStatManager) incTotalExceptions(err string) {
	sm.totalExceptions++
	var t time.Time
	sm.processTimeStart = t
	sm.lastException = err
	sm.lastExceptionTime = time.Now()
}

func (sm *DefaultStatManager) ProcessTimeStart() {
	sm.Lock()
	defer sm.Unlock()
	sm.lastInvocation = time.Now()
	sm.processTimeStart = sm.lastInvocation
}

func (sm *DefaultStatManager) ProcessTimeEnd() {
	sm.Lock()
	defer sm.Unlock()
	if !sm.processTimeStart.IsZero() {
		sm.processLatency = int64(time.Since(sm.processTimeStart) / time.Microsecond)
	}
}

func (sm *DefaultStatManager) SetBufferLength(l int64) {
	sm.Lock()
	defer sm.Unlock()
	sm.bufferLength = l
}

func (sm *DefaultStatManager) SetProcessTimeStart(t time.Time) {
	sm.Lock()
	defer sm.Unlock()
	sm.processTimeStart = t
	sm.lastInvocation = t
}

func (sm *DefaultStatManager) GetMetrics() []any {
	sm.RLock()
	defer sm.RUnlock()
	var result []any
	if sm.connectionState != nil {
		result = make([]any, 14)
	} else {
		result = make([]any, 9)
	}
	copy(result, []any{
		sm.totalRecordsIn,
		sm.totalRecordsOut,
		sm.totalMessagesProcessed,
		sm.processLatency,
		sm.bufferLength,
		int64(0),
		sm.totalExceptions,
		sm.lastException,
		int64(0),
	})

	if !sm.lastInvocation.IsZero() {
		result[5] = sm.lastInvocation.UnixMilli()
	}
	if !sm.lastExceptionTime.IsZero() {
		result[8] = sm.lastExceptionTime.UnixMilli()
	}
	if sm.connectionState != nil {
		result[9] = sm.connectionState.connStatus
		if !sm.connectionState.lastConnectedTime.IsZero() {
			result[10] = sm.connectionState.lastConnectedTime.UnixMilli()
		} else {
			result[10] = int64(0)
		}
		if !sm.connectionState.lastDisconnectTime.IsZero() {
			result[11] = sm.connectionState.lastDisconnectTime.UnixMilli()
		} else {
			result[11] = int64(0)
		}
		result[12] = sm.connectionState.lastDisconnect
		if !sm.connectionState.lastTryTime.IsZero() {
			result[13] = sm.connectionState.lastTryTime.UnixMilli()
		} else {
			result[13] = int64(0)
		}
	}
	return result
}

func (sm *DefaultStatManager) Clean(_ string) {
	// do nothing
}

type ConnectionStatManager struct {
	connStatus         int
	lastConnectedTime  time.Time
	lastTryTime        time.Time
	lastDisconnect     string
	lastDisconnectTime time.Time
}

func (csm *ConnectionStatManager) SetConnectionState(state string, message string) {
	setMemConnState(csm, state, message)
}

func setMemConnState(csm *ConnectionStatManager, state string, message string) {
	now := time.Now()
	switch state {
	case api.ConnectionDisconnected:
		csm.connStatus = -1
		csm.lastDisconnectTime = now
		csm.lastDisconnect = message
	case api.ConnectionConnecting:
		csm.connStatus = 0
		csm.lastTryTime = now
	case api.ConnectionConnected:
		csm.connStatus = 1
		csm.lastConnectedTime = now
	}
}
