package nodes

import (
	"fmt"
	"github.com/emqx/kuiper/xstream/api"
	"strconv"
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

func NewStatManager(opType string, ctx api.StreamContext) (*StatManager, error) {
	var prefix string
	switch opType {
	case "source":
		prefix = "kuiper_source_"
	case "op":
		prefix = "kuiper_op_"
	case "sink":
		prefix = "kuiper_sink_"
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

func (sm *StatManager) GetMetrics() map[string]interface{} {
	result := make(map[string]interface{})
	result[sm.prefix+sm.opId+"_"+strconv.Itoa(sm.instanceId)+"_"+RecordsInTotal] = sm.totalRecordsIn
	result[sm.prefix+sm.opId+"_"+strconv.Itoa(sm.instanceId)+"_"+RecordsOutTotal] = sm.totalRecordsOut
	result[sm.prefix+sm.opId+"_"+strconv.Itoa(sm.instanceId)+"_"+ExceptionsTotal] = sm.totalExceptions
	result[sm.prefix+sm.opId+"_"+strconv.Itoa(sm.instanceId)+"_"+LastInvocation] = sm.lastInvocation.String()
	result[sm.prefix+sm.opId+"_"+strconv.Itoa(sm.instanceId)+"_"+ProcessLatencyMs] = sm.processLatency

	return result
}
