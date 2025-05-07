// Copyright 2024-2025 EMQ Technologies Co., Ltd.
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

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/topo/checkpoint"
	kctx "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/topo/node"
	"github.com/lf-edge/ekuiper/v2/internal/topo/node/metric"
	"github.com/lf-edge/ekuiper/v2/internal/topo/state"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
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
	tail node.Emitter
	topo *def.PrintableTopo
	// Save the schemainfo for each rule only to use when need to attach schema when the rule is starting.
	// Get updated if the rule is updated. Never delete it until the subtopo is deleted.
	schemaReg map[string]schemainfo

	// runtime state
	// Ref state, affect the pool. Update when rule created or stopped
	refCount atomic.Int32
	refRules sync.Map // map[ruleId]errCh, notify the rule for errors
	// Runtime state, affect the running loop. Update when any rule opened or all rules stopped
	opened           atomic.Bool
	cancel           context.CancelFunc
	enableCheckpoint bool
}

func (s *SrcSubTopo) AddOutput(output chan interface{}, name string) error {
	return s.tail.AddOutput(output, name)
}

func (s *SrcSubTopo) RemoveOutput(name string) error {
	return s.tail.RemoveOutput(name)
}

func (s *SrcSubTopo) AddRef(ctx api.StreamContext, parentErrCh chan<- error) {
	if _, loaded := s.refRules.LoadOrStore(ctx.GetRuleId(), parentErrCh); !loaded {
		s.refCount.Add(1)
		ctx.GetLogger().Infof("Sub topo %s created for rule %s with %d ref", s.name, ctx.GetRuleId(), s.refCount.Load())
	} else {
		if parentErrCh != nil {
			s.refRules.Store(ctx.GetRuleId(), parentErrCh)
			ctx.GetLogger().Infof("Sub topo %s for rule %s opened with %d ref", s.name, ctx.GetRuleId(), s.refCount.Load())
		} else {
			s.refRules.Store(ctx.GetRuleId(), nil)
			ctx.GetLogger().Infof("Sub topo %s for rule %s reset with %d ref", s.name, ctx.GetRuleId(), s.refCount.Load())
		}
	}
}

func (s *SrcSubTopo) Open(ctx api.StreamContext, parentErrCh chan<- error) {
	// Update the ref count
	s.AddRef(ctx, parentErrCh)
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
			qos := def.AtMostOnce
			if s.enableCheckpoint {
				qos = def.AtLeastOnce
			}
			pctx, cancel, err := prepareSharedContext(ctx, s.name, qos)
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

func (s *SrcSubTopo) Close(ctx api.StreamContext, ruleId string, runId int) {
	if ch, ok := s.refRules.Load(ruleId); ok {
		// Only do clean up when rule is deleted instead of updated
		if ch != nil {
			s.refRules.Delete(ruleId)
			if s.refCount.CompareAndSwap(1, 0) {
				if s.cancel != nil {
					s.cancel()
				}
				if ss, ok := s.source.(*SrcSubTopo); ok {
					ss.Close(ctx, "$$subtopo_"+s.name, runId)
				}
				RemoveSubTopo(s.name)
			} else {
				s.refCount.Add(-1)
			}
			ctx.GetLogger().Infof("Sub topo %s dereference %s with %d ref", s.name, ctx.GetRuleId(), s.refCount.Load())
		}
		ctx.GetLogger().Infof("Sub topo %s update schema for rule %s change", s.name, ctx.GetRuleId())
		for _, op := range s.ops {
			if so, ok := op.(node.SchemaNode); ok {
				so.DetachSchema(ctx, ruleId)
			}
		}
	}
	_ = s.RemoveOutput(fmt.Sprintf("%s.%d", ruleId, runId))
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
	s.enableCheckpoint = true
}

func prepareSharedContext(parCtx api.StreamContext, k string, qos def.Qos) (api.StreamContext, context.CancelFunc, error) {
	contextLogger := conf.Log.WithField("subtopo", k)
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	if dParCtx, ok := parCtx.(*kctx.DefaultContext); ok {
		ctx.PropagateTracer(dParCtx)
	}
	ruleId := "$$subtopo_" + k
	opId := "subtopo_" + k
	store, err := state.CreateStore("subtopo_"+k, qos)
	if err != nil {
		ctx.GetLogger().Errorf("source pool %s create store error %v", k, err)
		return nil, nil, err
	}
	sctx, cancel := ctx.WithCancel()
	return sctx.WithMeta(ruleId, opId, store), cancel, nil
}

var (
	_ node.DataSourceNode          = &SrcSubTopo{}
	_ checkpoint.SourceSubTopoTask = &SrcSubTopo{}
)
