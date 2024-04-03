// Copyright 2024 EMQ Technologies Co., Ltd.
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

package promMetrics

import "github.com/prometheus/client_golang/prometheus"

const (
	LblStatusType = "status"
	LblRuleIDType = "ruleID"

	LBlRuleRunning = "running"
	LblRuleStop    = "stop"
)

var (
	RuleStatusCountGauge *prometheus.GaugeVec
	RuleStatusGauge      *prometheus.GaugeVec
)

func InitServerMetrics() {
	RuleStatusCountGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "kuiper",
		Subsystem: "rule",
		Name:      "count",
		Help:      "gauge of rule status count",
	}, []string{LblStatusType})

	RuleStatusGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "kuiper",
		Subsystem: "rule",
		Name:      "status",
		Help:      "gauge of rule status",
	}, []string{LblRuleIDType})
}

func RegisterMetrics() {
	InitServerMetrics()
	prometheus.MustRegister(RuleStatusCountGauge)
	prometheus.MustRegister(RuleStatusGauge)
}

func SetRuleStatusCountGauge(isRunning bool, count int) {
	lbl := LBlRuleRunning
	if !isRunning {
		lbl = LblRuleStop
	}
	RuleStatusCountGauge.WithLabelValues(lbl).Set(float64(count))
}

func SetRuleStatus(ruleID string, value int) {
	v := float64(value)
	RuleStatusGauge.WithLabelValues(ruleID).Set(v)
}

func RemoveRuleStatus(ruleID string) {
	RuleStatusGauge.DeleteLabelValues(ruleID)
}
