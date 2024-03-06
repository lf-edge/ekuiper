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
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/checkpoint"
	kctx "github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/node"
	"github.com/lf-edge/ekuiper/internal/topo/node/metric"
	"github.com/lf-edge/ekuiper/internal/topo/state"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/infra"
)

type schemainfo struct {
	datasource string
	schema     map[string]*ast.JsonStreamField
	isWildcard bool
}

// SrcSubTopo Implements node.SourceNode
type SrcSubTopo struct {
	name string

	// creation state
	source node.DataSourceNode
	// May be empty
	ops  []node.OperatorNode
	tail api.Emitter
	topo *api.PrintableTopo
	// Save the schemainfo for each rule only to use when need to attach schema when the rule is starting.
	// Get updated if the rule is updated. Never delete it until the subtopo is deleted.
	schemaReg map[string]schemainfo

	// runtime state
	// Ref state, affect the pool. Update when rule created or stopped
	refCount atomic.Int32
	refRules sync.Map // map[ruleId]errCh, notify the rule for errors
	// Runtime state, affect the running loop. Update when any rule opened or all rules stopped
	opened atomic.Bool
	cancel context.CancelFunc
}

func (s *SrcSubTopo) AddOutput(output chan<- interface{}, name string) error {
	return s.tail.AddOutput(output, name)
}

func (s *SrcSubTopo) Open(ctx api.StreamContext, parentErrCh chan<- error) {
	// Update the ref count
	if _, loaded := s.refRules.LoadOrStore(ctx.GetRuleId(), parentErrCh); !loaded {
		s.refCount.Add(1)
		ctx.GetLogger().Infof("Sub topo %s opened by rule %s with %d ref", s.name, ctx.GetRuleId(), s.refCount.Load())
	}
	// Attach schemas
	for _, op := range s.ops {
		if so, ok := op.(node.SchemaNode); ok {
			si, hasSchema := s.schemaReg[ctx.GetRuleId()]
			if hasSchema {
				ctx.GetLogger().Infof("attach schema to op %s", op.GetName())
				so.AttachSchema(ctx, si.datasource, si.schema, si.isWildcard)
			}
		}
	}
	// If not opened yet, open it. It may be opened before, but failed to open. In this case, try to open it again.
	if s.opened.CompareAndSwap(false, true) {
		poe := infra.SafeRun(func() error {
			ctx.GetLogger().Infof("Opening sub topo %s by rule %s", s.name, ctx.GetRuleId())
			pctx, cancel, err := prepareSharedContext(s.name)
			if err != nil {
				return err
			}
			errCh := make(chan error, 1)
			for _, op := range s.ops {
				op.Exec(pctx, errCh)
			}
			s.source.Open(pctx, errCh)
			s.cancel = cancel
			ctx.GetLogger().Infof("Sub topo %s opened by rule %s with 1 ref", s.name, ctx.GetRuleId())
			go func() {
				defer func() {
					conf.Log.Infof("Sub topo %s closed", s.name)
					s.opened.Store(false)
				}()
				for {
					select {
					case e := <-errCh:
						pctx.GetLogger().Infof("Sub topo %s exit for error %v", s.name, e)
						s.notifyError(e)
						return
					case <-pctx.Done():
						return
					}
				}
			}()
			return nil
		})
		if poe != nil {
			s.notifyError(poe)
		}
	}
}

func (s *SrcSubTopo) notifyError(poe error) {
	// Notify error to all ref rules
	s.refRules.Range(func(k, v interface{}) bool {
		conf.Log.Debugf("Notify error %v to rule %s", poe, k.(string))
		infra.DrainError(nil, poe, v.(chan<- error))
		return true
	})
}

func (s *SrcSubTopo) GetSource() node.DataSourceNode {
	return s.source
}

func (s *SrcSubTopo) GetName() string {
	return s.name
}

func (s *SrcSubTopo) SubMetrics() (keys []string, values []any) {
	for i, v := range s.source.GetMetrics() {
		keys = append(keys, fmt.Sprintf("source_%s_0_%s", s.source.GetName(), metric.MetricNames[i]))
		values = append(values, v)
	}
	for _, so := range s.ops {
		for i, v := range so.GetMetrics() {
			keys = append(keys, fmt.Sprintf("op_%s_%s_0_%s", s.name, so.GetName(), metric.MetricNames[i]))
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

func (s *SrcSubTopo) OpsCount() int {
	return len(s.ops)
}

func (s *SrcSubTopo) StoreSchema(ruleID, dataSource string, schema map[string]*ast.JsonStreamField, isWildCard bool) {
	s.schemaReg[ruleID] = schemainfo{
		datasource: dataSource,
		schema:     schema,
		isWildcard: isWildCard,
	}
}

func (s *SrcSubTopo) Close(ruleId string) {
	if _, ok := s.refRules.LoadAndDelete(ruleId); ok {
		s.refCount.Add(-1)
		if s.refCount.Load() == 0 {
			if s.cancel != nil {
				s.cancel()
			}
			RemoveSubTopo(s.name)
		}
		for _, op := range s.ops {
			if so, ok := op.(node.SchemaNode); ok {
				so.DetachSchema(ruleId)
			}
		}
	}
}

// RemoveMetrics is called when the rule is deleted
func (s *SrcSubTopo) RemoveMetrics(ruleId string) {
	if s.refCount.Load() == 0 {
		s.source.RemoveMetrics(ruleId)
		for _, op := range s.ops {
			op.RemoveMetrics(ruleId)
		}
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
