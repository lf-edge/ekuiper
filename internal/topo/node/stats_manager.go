package node

import (
	"fmt"
	"github.com/emqx/kuiper/internal/conf"
	"github.com/emqx/kuiper/pkg/api"
	"github.com/prometheus/client_golang/prometheus"
	"strconv"
	"time"
)

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

type PrometheusStatManager struct {
	DefaultStatManager
	//prometheus metrics
	pTotalRecordsIn  prometheus.Counter
	pTotalRecordsOut prometheus.Counter
	pTotalExceptions prometheus.Counter
	pProcessLatency  prometheus.Gauge
	pBufferLength    prometheus.Gauge
}

func NewStatManager(opType string, ctx api.StreamContext) (StatManager, error) {
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

	var sm StatManager
	if conf.Config != nil && conf.Config.Basic.Prometheus {
		ctx.GetLogger().Debugf("Create prometheus stat manager")
		psm := &PrometheusStatManager{
			DefaultStatManager: DefaultStatManager{
				opType:     opType,
				prefix:     prefix,
				opId:       ctx.GetOpId(),
				instanceId: ctx.GetInstanceId(),
			},
		}
		//assign prometheus
		mg := GetPrometheusMetrics().GetMetricsGroup(opType)
		strInId := strconv.Itoa(ctx.GetInstanceId())
		psm.pTotalRecordsIn = mg.TotalRecordsIn.WithLabelValues(ctx.GetRuleId(), opType, ctx.GetOpId(), strInId)
		psm.pTotalRecordsOut = mg.TotalRecordsOut.WithLabelValues(ctx.GetRuleId(), opType, ctx.GetOpId(), strInId)
		psm.pTotalExceptions = mg.TotalExceptions.WithLabelValues(ctx.GetRuleId(), opType, ctx.GetOpId(), strInId)
		psm.pProcessLatency = mg.ProcessLatency.WithLabelValues(ctx.GetRuleId(), opType, ctx.GetOpId(), strInId)
		psm.pBufferLength = mg.BufferLength.WithLabelValues(ctx.GetRuleId(), opType, ctx.GetOpId(), strInId)
		sm = psm
	} else {
		sm = &DefaultStatManager{
			opType:     opType,
			prefix:     prefix,
			opId:       ctx.GetOpId(),
			instanceId: ctx.GetInstanceId(),
		}
	}
	return sm, nil
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

func (sm *PrometheusStatManager) IncTotalRecordsIn() {
	sm.totalRecordsIn++
	sm.pTotalRecordsIn.Inc()
}

func (sm *PrometheusStatManager) IncTotalRecordsOut() {
	sm.totalRecordsOut++
	sm.pTotalRecordsOut.Inc()
}

func (sm *PrometheusStatManager) IncTotalExceptions() {
	sm.totalExceptions++
	sm.pTotalExceptions.Inc()
	var t time.Time
	sm.processTimeStart = t
}

func (sm *PrometheusStatManager) ProcessTimeEnd() {
	if !sm.processTimeStart.IsZero() {
		sm.processLatency = int64(time.Since(sm.processTimeStart) / time.Microsecond)
		sm.pProcessLatency.Set(float64(sm.processLatency))
	}
}

func (sm *PrometheusStatManager) SetBufferLength(l int64) {
	sm.bufferLength = l
	sm.pBufferLength.Set(float64(l))
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
