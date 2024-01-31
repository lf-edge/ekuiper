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

package topo

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/checkpoint"
	kctx "github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/node"
	"github.com/lf-edge/ekuiper/internal/topo/node/metric"
	"github.com/lf-edge/ekuiper/internal/topo/state"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/infra"
)

// SrcSubTopo Implements node.SourceNode
type SrcSubTopo struct {
	name string

	// creation state
	source node.DataSourceNode
	ops    []node.OperatorNode
	tail   api.Emitter
	topo   *api.PrintableTopo

	// runtime state
	refCount atomic.Int32
	refRules sync.Map
	cancel   context.CancelFunc
}

func (s *SrcSubTopo) AddOutput(output chan<- interface{}, name string) error {
	return s.tail.AddOutput(output, name)
}

func (s *SrcSubTopo) Open(ctx api.StreamContext, errCh chan<- error) {
	ruleId := ctx.GetRuleId()
	ctx.GetLogger().Infof("Opening sub topo %s by rule %s", s.name, ruleId)
	if s.refCount.Load() == 0 {
		pctx, cancel, err := prepareSharedContext(s.name)
		if err != nil {
			infra.DrainError(ctx, err, errCh)
			return
		}
		for _, op := range s.ops {
			op.Exec(pctx, errCh)
		}
		s.source.Open(pctx, errCh)
		s.cancel = cancel
		s.refCount.Add(1)
		s.refRules.Store(ruleId, true)
		ctx.GetLogger().Infof("Sub topo %s opened by rule %s with 1 ref", s.name, ruleId)
	} else if _, loaded := s.refRules.LoadOrStore(ruleId, true); !loaded {
		s.refCount.Add(1)
		ctx.GetLogger().Infof("Sub topo %s opened by rule %s with %d ref", s.name, ruleId, s.refCount.Load())
	}
}

func (s *SrcSubTopo) GetSource() node.DataSourceNode {
	return s.source
}

func (s *SrcSubTopo) GetName() string {
	return s.name
}

func (s *SrcSubTopo) SubMetrics() (keys []string, values []any) {
	for i, v := range s.source.GetMetrics() {
		keys = append(keys, "source_subtopo_"+s.source.GetName()+"_0_"+metric.MetricNames[i])
		values = append(values, v)
	}
	for _, so := range s.ops {
		for i, v := range so.GetMetrics() {
			keys = append(keys, "op_subtopo_"+so.GetName()+"_0_"+metric.MetricNames[i])
			values = append(values, v)
		}
	}
	return
}

func (s *SrcSubTopo) GetMetrics() []any {
	result := s.source.GetMetrics()
	for _, op := range s.ops {
		result = append(result, op.GetMetrics()...)
	}
	return result
}

// RemoveMetrics is called when the rule is deleted
func (s *SrcSubTopo) RemoveMetrics(ruleId string) {
	s.refCount.Add(-1)
	if s.refCount.Load() == 0 {
		s.cancel()
		s.source.RemoveMetrics(ruleId)
		for _, op := range s.ops {
			op.RemoveMetrics(ruleId)
		}
		RemoveSubTopo(s.name)
	}
}

func (s *SrcSubTopo) EnableCheckpoint(sources *[]checkpoint.StreamTask, ops *[]checkpoint.NonSourceTask) {
	*sources = append(*sources, s.source.(checkpoint.StreamTask))
	for _, op := range s.ops {
		*ops = append(*ops, op)
	}
}

func prepareSharedContext(k string) (api.StreamContext, context.CancelFunc, error) {
	contextLogger := conf.Log.WithField("subtopo", k)
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	ruleId := "$$subtopo_" + k
	opId := "subtopo_" + k
	store, err := state.CreateStore("subtopo_"+k, 0)
	if err != nil {
		ctx.GetLogger().Errorf("source pool %s create store error %v", k, err)
		return nil, nil, err
	}
	sctx, cancel := ctx.WithMeta(ruleId, opId, store).WithCancel()
	return sctx, cancel, nil
}

var (
	_ node.DataSourceNode          = &SrcSubTopo{}
	_ checkpoint.SourceSubTopoTask = &SrcSubTopo{}
)
