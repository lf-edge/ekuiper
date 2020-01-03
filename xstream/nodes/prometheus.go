package nodes

import (
	"github.com/prometheus/client_golang/prometheus"
	"sync"
)

const RecordsInTotal = "records_in_total"
const RecordsOutTotal = "records_out_total"
const ExceptionsTotal = "exceptions_total"
const ProcessLatencyMs = "process_latency_ms"
const LastInvocation = "last_invocation"
const BufferLength = "buffer_length"

var (
	MetricNames = []string{RecordsInTotal, RecordsOutTotal, ExceptionsTotal, ProcessLatencyMs, BufferLength, LastInvocation}
	prometheuseMetrics *PrometheusMetrics
	mutex sync.RWMutex
)

func GetPrometheusMetrics() *PrometheusMetrics{
	mutex.Lock()
	if prometheuseMetrics == nil{
		prometheuseMetrics = newPrometheusMetrics()
	}
	mutex.Unlock()
	return prometheuseMetrics
}

type MetricGroup struct{
	TotalRecordsIn *prometheus.CounterVec
	TotalRecordsOut *prometheus.CounterVec
	TotalExceptions *prometheus.CounterVec
	ProcessLatency *prometheus.GaugeVec
	BufferLength   *prometheus.GaugeVec
}

type PrometheusMetrics struct {
	vecs []*MetricGroup
}

func newPrometheusMetrics() *PrometheusMetrics{
	var (
		labelNames = []string{ "rule", "type", "op", "instance" }
		prefixes = []string{"source", "op", "sink"}
	)
	var vecs []*MetricGroup
	for _, prefix := range prefixes {
		//prometheus initialization
		totalRecordsIn := prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: prefix + "_" + RecordsInTotal,
			Help: "Total number of messages received by the operation of " + prefix,
		}, labelNames)
		totalRecordsOut := prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: prefix + "_" + RecordsOutTotal,
			Help: "Total number of messages published by the operation of " + prefix,
		}, labelNames)
		totalExceptions := prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: prefix + "_" + ExceptionsTotal,
			Help: "Total number of user exceptions of " + prefix,
		}, labelNames)
		processLatency := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: prefix + "_" + ProcessLatencyMs,
			Help: "Process latency in millisecond of " + prefix,
		}, labelNames)
		bufferLength := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: prefix + "_" + BufferLength,
			Help: "The length of the plan buffer which is shared by all instances of " + prefix,
		}, labelNames)
		prometheus.MustRegister(totalRecordsIn, totalRecordsOut, totalExceptions, processLatency, bufferLength)
		vecs = append(vecs, &MetricGroup{
			TotalRecordsIn:  totalRecordsIn,
			TotalRecordsOut: totalRecordsOut,
			TotalExceptions: totalExceptions,
			ProcessLatency:  processLatency,
			BufferLength:    bufferLength,
		})
	}
	return &PrometheusMetrics{vecs:vecs}
}

func (m *PrometheusMetrics) GetMetricsGroup(opType string) *MetricGroup{
	switch opType{
	case "source":
		return m.vecs[0]
	case "op":
		return m.vecs[1]
	case "sink":
		return m.vecs[2]
	}
	return nil
}