package nodes

import (
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xstream/api"
	"github.com/emqx/kuiper/xstream/contexts"
	"github.com/prometheus/client_golang/prometheus"
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
	bufferLength    int64
	//prometheus metrics
	pTotalRecordsIn  prometheus.Counter
	pTotalRecordsOut prometheus.Counter
	pTotalExceptions prometheus.Counter
	pProcessLatency  prometheus.Gauge
	pLastInvocation  prometheus.Gauge
	pBufferLength	 prometheus.Gauge
	//configs
	opType           string //"source", "op", "sink"
	prefix           string
	processTimeStart time.Time
	opId             string
	instanceId		 int
}

var labelNames = []string{ "rule", "type", "op", "instance" }

const RecordsInTotal = "records_in_total"
const RecordsOutTotal = "records_out_total"
const ExceptionsTotal = "exceptions_total"
const ProcessLatencyMs = "process_latency_ms"
const LastInvocation = "last_invocation"
const BufferLength   = "buffer_length"

var MetricNames = []string{RecordsInTotal, RecordsOutTotal, ExceptionsTotal, ProcessLatencyMs, BufferLength, LastInvocation}

func NewStatManager(opType string, ctx api.StreamContext) (*StatManager, error) {
	r, ok := ctx.Value(contexts.CollectorRegistry).(prometheus.Registry)
	if !ok {
		return nil, fmt.Errorf("cannot find prometheus registry in context")
	}
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
	//prometheus initialization
	totalRecordsIn := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: prefix + RecordsInTotal,
		Help: "Total number of messages received by the operation",
	}, labelNames)
	totalRecordsOut := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: prefix + RecordsOutTotal,
		Help: "Total number of messages published by the operation",
	}, labelNames)
	totalExceptions := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: prefix + ExceptionsTotal,
		Help: "Total number of user exceptions",
	}, labelNames)
	processLatency := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: prefix + ProcessLatencyMs,
		Help: "Process latency in millisecond",
	}, labelNames)
	lastInvocation := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: prefix + LastInvocation,
		Help: "The timestamp of the last invocation of the operation",
	}, labelNames)
	bufferLength := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: prefix + BufferLength,
		Help: "The length of the plan buffer which is shared by all instances",
	}, labelNames)
	r.MustRegister(totalRecordsIn, totalRecordsOut, totalExceptions, processLatency, lastInvocation, bufferLength)
	sm := &StatManager{
		opType: opType,
		prefix: prefix,
		opId:   ctx.GetOpId(),
		instanceId: ctx.GetInstanceId(),
	}
	//assign prometheus
	strInId := strconv.Itoa(ctx.GetInstanceId())
	sm.pTotalRecordsIn = totalRecordsIn.WithLabelValues(ctx.GetRuleId(), opType, ctx.GetOpId(), strInId)
	sm.pTotalRecordsOut = totalRecordsOut.WithLabelValues(ctx.GetRuleId(), opType, ctx.GetOpId(), strInId)
	sm.pTotalExceptions = totalExceptions.WithLabelValues(ctx.GetRuleId(), opType, ctx.GetOpId(), strInId)
	sm.pProcessLatency = processLatency.WithLabelValues(ctx.GetRuleId(), opType, ctx.GetOpId(), strInId)
	sm.pLastInvocation = lastInvocation.WithLabelValues(ctx.GetRuleId(), opType, ctx.GetOpId(), strInId)
	sm.pBufferLength   = bufferLength.WithLabelValues(ctx.GetRuleId(), opType, ctx.GetOpId(), strInId)
	return sm, nil
}

func (sm *StatManager) IncTotalRecordsIn() {
	sm.totalRecordsIn++
	sm.pTotalRecordsIn.Inc()
}

func (sm *StatManager) IncTotalRecordsOut() {
	sm.totalRecordsOut++
	sm.pTotalRecordsOut.Inc()
}

func (sm *StatManager) IncTotalExceptions() {
	sm.totalExceptions++
	sm.pTotalExceptions.Inc()
	var t time.Time
	sm.processTimeStart = t
}

func (sm *StatManager) ProcessTimeStart() {
	sm.lastInvocation = time.Now()
	sm.processTimeStart = sm.lastInvocation
	sm.pLastInvocation.Set(float64(common.TimeToUnixMilli(sm.lastInvocation)))
}

func (sm *StatManager) ProcessTimeEnd() {
	if !sm.processTimeStart.IsZero() {
		sm.processLatency = int64(time.Since(sm.processTimeStart) / time.Millisecond)
		sm.pProcessLatency.Set(float64(sm.processLatency))
	}
}

func (sm *StatManager) SetBufferLength(l int64) {
	sm.bufferLength = l
	sm.pBufferLength.Set(float64(l))
}

func (sm *StatManager) GetMetrics() []interface{} {
	result := []interface{}{
		sm.totalRecordsIn, sm.totalRecordsOut, sm.totalExceptions, sm.processLatency, sm.bufferLength,
	}

	if !sm.lastInvocation.IsZero(){
		result = append(result, sm.lastInvocation.Format("2006-01-02T15:04:05.999999"))
	}

	return result
}
