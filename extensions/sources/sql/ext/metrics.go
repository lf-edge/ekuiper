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

package sql

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/lf-edge/ekuiper/metrics"
)

const (
	LblRecon = "recon"
	LblQuery = "query"
	LblScan  = "scan"
	LblWait  = "wait"
)

var (
	SqlSourceCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kuiper",
		Subsystem: "sql_source",
		Name:      "counter",
		Help:      "counter of SQL Source IO",
	}, []string{metrics.LblType, metrics.LblRuleIDType, metrics.LblOpIDType})

	SqlSourceQueryDurationHist = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "kuiper",
		Subsystem: "sql_source",
		Name:      "query_duration_microseconds",
		Help:      "SQL Source Historgram Duration of IO",
		Buckets:   prometheus.ExponentialBuckets(10, 2, 20), // 10us ~ 5s
	}, []string{metrics.LblType, metrics.LblRuleIDType, metrics.LblOpIDType})
)

func init() {
	prometheus.MustRegister(SqlSourceCounter)
	prometheus.MustRegister(SqlSourceQueryDurationHist)
}
