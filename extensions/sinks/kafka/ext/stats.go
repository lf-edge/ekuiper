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
	"time"

	"github.com/lf-edge/ekuiper/pkg/api"
)

type KafkaCollectStats struct {
	TotalBuildMsgDuration     time.Duration
	TotalTransformMsgDuration time.Duration
	TotalCollectMsgDuration   time.Duration
}

func (m *kafkaSink) ResetStats() {
	m.LastCollectStats = &KafkaCollectStats{}
}

func (m *kafkaSink) updateMetrics(ctx api.StreamContext) {
	KafkaSinkCollectDurationHist.WithLabelValues(lblBuild, LblReq, ctx.GetRuleId(), ctx.GetOpId()).Observe(float64(m.LastCollectStats.TotalBuildMsgDuration.Microseconds()))
	KafkaSinkCollectDurationHist.WithLabelValues(LblTransform, LblReq, ctx.GetRuleId(), ctx.GetOpId()).Observe(float64(m.LastCollectStats.TotalTransformMsgDuration.Microseconds()))
	KafkaSinkCollectDurationHist.WithLabelValues(LblCollect, LblReq, ctx.GetRuleId(), ctx.GetOpId()).Observe(float64(m.LastCollectStats.TotalCollectMsgDuration.Microseconds()))
}
