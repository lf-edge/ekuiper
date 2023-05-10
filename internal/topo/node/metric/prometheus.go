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

//go:build prometheus || !core

package metric

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	prometheuseMetrics *PrometheusMetrics
	mutex              sync.RWMutex
)

func GetPrometheusMetrics() *PrometheusMetrics {
	mutex.Lock()
	if prometheuseMetrics == nil {
		prometheuseMetrics = newPrometheusMetrics()
	}
	mutex.Unlock()
	return prometheuseMetrics
}

type MetricGroup struct {
	TotalRecordsIn  *prometheus.CounterVec
	TotalRecordsOut *prometheus.CounterVec
	TotalExceptions *prometheus.CounterVec
	ProcessLatency  *prometheus.GaugeVec
	BufferLength    *prometheus.GaugeVec
}

type PrometheusMetrics struct {
	vecs []*MetricGroup
}

func newPrometheusMetrics() *PrometheusMetrics {
	var (
		labelNames = []string{"rule", "type", "op", "instance"}
		prefixes   = []string{"kuiper_source", "kuiper_op", "kuiper_sink"}
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
			Name: prefix + "_" + ProcessLatencyUs,
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
	return &PrometheusMetrics{vecs: vecs}
}

func (m *PrometheusMetrics) GetMetricsGroup(opType string) *MetricGroup {
	switch opType {
	case "source":
		return m.vecs[0]
	case "op":
		return m.vecs[1]
	case "sink":
		return m.vecs[2]
	}
	return nil
}
