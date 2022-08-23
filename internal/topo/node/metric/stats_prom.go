// Copyright 2022 EMQ Technologies Co., Ltd.
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
// +build prometheus !core

package metric

import (
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/prometheus/client_golang/prometheus"
	"strconv"
	"time"
)

func getStatManager(ctx api.StreamContext, dsm DefaultStatManager) (StatManager, error) {
	ctx.GetLogger().Debugf("Create prometheus stat manager")
	var sm StatManager
	if conf.Config != nil && conf.Config.Basic.Prometheus {
		psm := &PrometheusStatManager{
			DefaultStatManager: dsm,
		}
		//assign prometheus
		mg := GetPrometheusMetrics().GetMetricsGroup(dsm.opType)
		strInId := strconv.Itoa(dsm.instanceId)
		psm.pTotalRecordsIn = mg.TotalRecordsIn.WithLabelValues(ctx.GetRuleId(), dsm.opType, dsm.opId, strInId)
		psm.pTotalRecordsOut = mg.TotalRecordsOut.WithLabelValues(ctx.GetRuleId(), dsm.opType, dsm.opId, strInId)
		psm.pTotalExceptions = mg.TotalExceptions.WithLabelValues(ctx.GetRuleId(), dsm.opType, dsm.opId, strInId)
		psm.pProcessLatency = mg.ProcessLatency.WithLabelValues(ctx.GetRuleId(), dsm.opType, dsm.opId, strInId)
		psm.pBufferLength = mg.BufferLength.WithLabelValues(ctx.GetRuleId(), dsm.opType, dsm.opId, strInId)
		sm = psm
	} else {
		sm = &dsm
	}
	return sm, nil
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

func (sm *PrometheusStatManager) IncTotalRecordsIn() {
	sm.totalRecordsIn++
	sm.pTotalRecordsIn.Inc()
}

func (sm *PrometheusStatManager) IncTotalRecordsOut() {
	sm.totalRecordsOut++
	sm.pTotalRecordsOut.Inc()
}

func (sm *PrometheusStatManager) IncTotalExceptions(err string) {
	sm.pTotalExceptions.Inc()
	sm.DefaultStatManager.IncTotalExceptions(err)
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
