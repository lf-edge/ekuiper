package nodes

import (
	"fmt"
	"github.com/emqx/kuiper/xstream/api"
	"time"
)

//The statManager is not thread safe. Make sure it is used in only one instance
type StatManager struct {
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
	instanceId		 int
}

const RecordsInTotal = "records_in_total"
const RecordsOutTotal = "records_out_total"
const ExceptionsTotal = "exceptions_total"
const ProcessLatencyMs = "process_latency_ms"
const LastInvocation = "last_invocation"
const BufferLength   = "buffer_length"

var MetricNames = []string{RecordsInTotal, RecordsOutTotal, ExceptionsTotal, ProcessLatencyMs, BufferLength, LastInvocation}

func NewStatManager(opType string, ctx api.StreamContext) (*StatManager, error) {
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
	sm := &StatManager{
		opType: opType,
		prefix: prefix,
		opId:   ctx.GetOpId(),
		instanceId: ctx.GetInstanceId(),
	}
	return sm, nil
}

func (sm *StatManager) IncTotalRecordsIn() {
	sm.totalRecordsIn++
}

func (sm *StatManager) IncTotalRecordsOut() {
	sm.totalRecordsOut++
}

func (sm *StatManager) IncTotalExceptions() {
	sm.totalExceptions++
	var t time.Time
	sm.processTimeStart = t
}

func (sm *StatManager) ProcessTimeStart() {
	sm.lastInvocation = time.Now()
	sm.processTimeStart = sm.lastInvocation
}

func (sm *StatManager) ProcessTimeEnd() {
	if !sm.processTimeStart.IsZero() {
		sm.processLatency = int64(time.Since(sm.processTimeStart) / time.Millisecond)
	}
}

func (sm *StatManager) SetBufferLength(l int64) {
	sm.bufferLength = l
}

func (sm *StatManager) GetMetrics() []interface{} {
	result := []interface{}{
		sm.totalRecordsIn, sm.totalRecordsOut, sm.totalExceptions, sm.processLatency, sm.bufferLength,
	}

	if !sm.lastInvocation.IsZero(){
		result = append(result, sm.lastInvocation.Format("2006-01-02T15:04:05.999999"))
	}else{
		result = append(result, 0)
	}

	return result
}
