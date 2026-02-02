// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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

package metric

import (
	"strconv"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
)

func getStatManager(ctx api.StreamContext, dsm *DefaultStatManager) (StatManager, error) {
	ctx.GetLogger().Debugf("Create prometheus stat manager")
	var sm StatManager
	if conf.Config != nil && conf.Config.Basic.Prometheus {
		psm := &PrometheusStatManager{
			DefaultStatManager: dsm,
		}
		// assign prometheus
		mg := GetPrometheusMetrics().GetMetricsGroup(dsm.opType)
		strInId := strconv.Itoa(dsm.instanceId)
		mg.TotalRecordsIn.DeleteLabelValues(ctx.GetRuleId(), dsm.opType, dsm.opId, strInId)
		mg.TotalMessagesProcessed.DeleteLabelValues(ctx.GetRuleId(), dsm.opType, dsm.opId, strInId)
		mg.TotalRecordsOut.DeleteLabelValues(ctx.GetRuleId(), dsm.opType, dsm.opId, strInId)
		mg.TotalExceptions.DeleteLabelValues(ctx.GetRuleId(), dsm.opType, dsm.opId, strInId)
		mg.ProcessLatency.DeleteLabelValues(ctx.GetRuleId(), dsm.opType, dsm.opId, strInId)
		mg.ProcessLatencyHist.DeleteLabelValues(ctx.GetRuleId(), dsm.opType, dsm.opId, strInId)
		mg.BufferLength.DeleteLabelValues(ctx.GetRuleId(), dsm.opType, dsm.opId, strInId)
		if mg.ConnectionStatus != nil {
			mg.ConnectionStatus.DeleteLabelValues(ctx.GetRuleId(), dsm.opType, dsm.opId, strInId)
		}

		psm.pTotalRecordsIn = mg.TotalRecordsIn.WithLabelValues(ctx.GetRuleId(), dsm.opType, dsm.opId, strInId)
		psm.pTotalMessagesProcessed = mg.TotalMessagesProcessed.WithLabelValues(ctx.GetRuleId(), dsm.opType, dsm.opId, strInId)
		psm.pTotalRecordsOut = mg.TotalRecordsOut.WithLabelValues(ctx.GetRuleId(), dsm.opType, dsm.opId, strInId)
		psm.pTotalExceptions = mg.TotalExceptions.WithLabelValues(ctx.GetRuleId(), dsm.opType, dsm.opId, strInId)
		psm.pProcessLatency = mg.ProcessLatency.WithLabelValues(ctx.GetRuleId(), dsm.opType, dsm.opId, strInId)
		psm.pProcessLatencyHist = mg.ProcessLatencyHist.WithLabelValues(ctx.GetRuleId(), dsm.opType, dsm.opId, strInId)
		psm.pBufferLength = mg.BufferLength.WithLabelValues(ctx.GetRuleId(), dsm.opType, dsm.opId, strInId)
		if dsm.opType != "op" {
			psm.pConnectionStatus = mg.ConnectionStatus.WithLabelValues(ctx.GetRuleId(), dsm.opType, dsm.opId, strInId)
		}
		sm = psm
	} else {
		sm = dsm
	}
	return sm, nil
}

type PrometheusStatManager struct {
	*DefaultStatManager
	// prometheus metrics
	pTotalMessagesProcessed prometheus.Counter
	pTotalRecordsIn         prometheus.Counter
	pTotalRecordsOut        prometheus.Counter
	pTotalExceptions        prometheus.Counter
	pProcessLatency         prometheus.Gauge
	pProcessLatencyHist     prometheus.Observer
	pBufferLength           prometheus.Gauge
	pConnectionStatus       prometheus.Gauge
}

func (sm *PrometheusStatManager) IncTotalRecordsIn() {
	sm.Lock()
	defer sm.Unlock()
	sm.totalRecordsIn++
	sm.pTotalRecordsIn.Inc()
}

func (sm *PrometheusStatManager) IncTotalMessagesProcessed(n int64) {
	sm.Lock()
	defer sm.Unlock()
	sm.totalMessagesProcessed++
	sm.pTotalMessagesProcessed.Add(float64(n))
}

func (sm *PrometheusStatManager) IncTotalRecordsOut() {
	sm.Lock()
	defer sm.Unlock()
	sm.totalRecordsOut++
	sm.pTotalRecordsOut.Inc()
}

func (sm *PrometheusStatManager) IncTotalExceptions(err string) {
	sm.Lock()
	defer sm.Unlock()
	sm.pTotalExceptions.Inc()
	sm.DefaultStatManager.incTotalExceptions(err)
}

func (sm *PrometheusStatManager) ProcessTimeEnd() {
	sm.Lock()
	defer sm.Unlock()
	if !sm.processTimeStart.IsZero() {
		sm.processLatency = int64(time.Since(sm.processTimeStart) / time.Microsecond)
		sm.pProcessLatency.Set(float64(sm.processLatency))
		sm.pProcessLatencyHist.Observe(float64(sm.processLatency))
	}
}

func (sm *PrometheusStatManager) SetBufferLength(l int64) {
	sm.Lock()
	defer sm.Unlock()
	sm.bufferLength = l
	sm.pBufferLength.Set(float64(l))
}

func (sm *PrometheusStatManager) Clean(ruleId string) {
	if conf.Config != nil && conf.Config.Basic.Prometheus {
		mg := GetPrometheusMetrics().GetMetricsGroup(sm.opType)
		strInId := strconv.Itoa(sm.instanceId)
		mg.TotalRecordsIn.DeleteLabelValues(ruleId, sm.opType, sm.opId, strInId)
		mg.TotalRecordsOut.DeleteLabelValues(ruleId, sm.opType, sm.opId, strInId)
		mg.TotalMessagesProcessed.DeleteLabelValues(ruleId, sm.opType, sm.opId, strInId)
		mg.TotalExceptions.DeleteLabelValues(ruleId, sm.opType, sm.opId, strInId)
		mg.ProcessLatency.DeleteLabelValues(ruleId, sm.opType, sm.opId, strInId)
		mg.BufferLength.DeleteLabelValues(ruleId, sm.opType, sm.opId, strInId)
		if mg.ConnectionStatus != nil {
			mg.ConnectionStatus.DeleteLabelValues(ruleId, sm.opType, sm.opId, strInId)
		}
		conf.Log.Debugf("finish removing rule:%v, opType:%v, opId:%v, InId:%v prometheus metrics", ruleId, sm.opType, sm.opId, strInId)
	}
}

func (sm *PrometheusStatManager) SetConnectionState(state string, message string) {
	sm.Lock()
	defer sm.Unlock()
	switch state {
	case api.ConnectionDisconnected:
		sm.pConnectionStatus.Set(-1)
	case api.ConnectionConnecting:
		sm.pConnectionStatus.Set(0)
	case api.ConnectionConnected:
		sm.pConnectionStatus.Set(1)
	}
	setMemConnState(sm.connectionState, state, message)
}
