// Copyright 2025 EMQ Technologies Co., Ltd.
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

package kafka

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/lf-edge/ekuiper/v2/metrics"
)

const (
	LblTarget = "target"
)

var (
	KafkaSinkCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kuiper",
		Subsystem: "kafka_sink",
		Name:      "counter",
		Help:      "counter of Kafka Sink IO",
	}, []string{metrics.LblType, LblTarget, metrics.LblRuleIDType, metrics.LblOpIDType})

	KafkaSinkCollectDurationHist = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "kuiper",
		Subsystem: "kafka_sink",
		Name:      "collect_duration_hist",
		Help:      "Sink Historgram Duration of IO",
		Buckets:   prometheus.ExponentialBuckets(10, 2, 20), // 10us ~ 5s
	}, []string{metrics.LblType, LblTarget, metrics.LblRuleIDType, metrics.LblOpIDType})

	KafkaSourceCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kuiper",
		Subsystem: "kafka_source",
		Name:      "counter",
		Help:      "counter of Kafka Source IO",
	}, []string{metrics.LblType, metrics.LblRuleIDType, metrics.LblOpIDType})

	KafkaSourceGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "kuiper",
		Subsystem: "kafka_source",
		Name:      "gauge",
		Help:      "Gauge of Kafka Source IO",
	}, []string{metrics.LblType, metrics.LblRuleIDType, metrics.LblOpIDType})
)

func init() {
	prometheus.MustRegister(KafkaSinkCounter)
	prometheus.MustRegister(KafkaSinkCollectDurationHist)
	prometheus.MustRegister(KafkaSourceCounter)
	prometheus.MustRegister(KafkaSourceGauge)
}
