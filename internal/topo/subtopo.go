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
	"sync/atomic"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/topo/checkpoint"
	kctx "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/topo/node"
	"github.com/lf-edge/ekuiper/v2/internal/topo/node/metric"
	"github.com/lf-edge/ekuiper/v2/internal/topo/schema"
	"github.com/lf-edge/ekuiper/v2/internal/topo/state"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
	"github.com/lf-edge/ekuiper/v2/pkg/syncx"
)

// SrcSubTopo Implements node.SourceNode
type SrcSubTopo struct {
	name        string
	isSliceMode bool

	// creation state
	source node.DataSourceNode
	// May be empty
	ops         []node.OperatorNode
	tail        node.Emitter
	topo        *def.PrintableTopo
	schemaLayer *schema.SharedLayer
	// runtime state
	// Ref state, affect the pool. Update when rule created or stopped
	syncx.RWMutex
	refRules map[string]chan<- error // map[ruleId]errCh, notify the rule for errors
	// Runtime state, affect the running loop. Update when any rule opened or all rules stopped
	opened           atomic.Bool
	cancel           context.CancelFunc
	enableCheckpoint bool
}

// IsSliceMode this is a constant set when creating new subtopo
func (s *SrcSubTopo) IsSliceMode() bool {
	return s.isSliceMode
}

func (s *SrcSubTopo) AddOutput(output chan interface{}, name string) error {
	return s.tail.AddOutput(output, name)
}

func (s *SrcSubTopo) RemoveOutput(name string) error {
	if s.tail != nil {
		return s.tail.RemoveOutput(name)
	}
	return nil
}

func (s *SrcSubTopo) AddRef(ctx api.StreamContext, parentErrCh chan<- error) {
	s.Lock()
	defer s.Unlock()
	if _, ok := s.refRules[ctx.GetRuleId()]; !ok {
		ctx.GetLogger().Infof("Sub topo %s created for rule %s with %d ref", s.name, ctx.GetRuleId(), len(s.refRules))
	} else {
		if parentErrCh != nil {
			ctx.GetLogger().Infof("Sub topo %s for rule %s opened with %d ref", s.name, ctx.GetRuleId(), len(s.refRules))
		} else {
			ctx.GetLogger().Infof("Sub topo %s for rule %s reset with %d ref", s.name, ctx.GetRuleId(), len(s.refRules))
		}
	}
	s.refRules[ctx.GetRuleId()] = parentErrCh
}

func (s *SrcSubTopo) Open(ctx api.StreamContext, parentErrCh chan<- error) {
	// Update the ref count
	s.AddRef(ctx, parentErrCh)
	// Attach schemas
	err := s.schemaLayer.Attach(ctx)
	if err != nil {
		ctx.GetLogger().Warnf("attach schema layer failed: %s", err)
	}
	for _, op := range s.ops {
		if so, ok := op.(node.SchemaNode); ok {
			ctx.GetLogger().Infof("reset schema to op %s", op.GetName())
			so.ResetSchema(ctx, s.schemaLayer.GetSchema())
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
			if s.source != nil {
				s.source.Open(pctx, errCh)
			}
			s.cancel = cancel
			ctx.GetLogger().Infof("Sub topo %s opened by rule %s with 1 ref", s.name, ctx.GetRuleId())
			go func() {
				defer func() {
					s.opened.Store(false)
					conf.Log.Infof("Sub topo %s closed", s.name)
				}()
				for {
					select {
					case e := <-errCh:
						pctx.GetLogger().Infof("Sub topo %s exit for error %v", s.name, e)
						s.opened.Store(false)
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
			s.opened.Store(false)
			s.notifyError(poe)
		}
	}
}

func (s *SrcSubTopo) notifyError(poe error) {
	s.RLock()
	defer s.RUnlock()
	// Notify error to all ref rules
	for k, ch := range s.refRules {
		conf.Log.Debugf("Notify error %v to rule %s", poe, k)
		infra.DrainError(nil, poe, ch)
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
	s.schemaLayer.RegSchema(ruleID, dataSource, schema, isWildCard)
}

func (s *SrcSubTopo) Close(ctx api.StreamContext, ruleId string, runId int) {
	s.Lock()
	requiresCleanup := false
	if ch, ok := s.refRules[ruleId]; ok {
		isStopped := ch != nil
		// Only do clean up when rule is deleted instead of updated
		if isStopped {
			delete(s.refRules, ruleId)
			if len(s.refRules) == 0 {
				requiresCleanup = true
			}
			ctx.GetLogger().Infof("Sub topo %s dereference %s with %d ref", s.name, ctx.GetRuleId(), len(s.refRules))
		}
		ctx.GetLogger().Infof("Sub topo %s update schema for rule %s change", s.name, ctx.GetRuleId())
		err := s.schemaLayer.Detach(ctx, isStopped)
		if err != nil {
			ctx.GetLogger().Warnf("detach schema layer failed: %s", err)
		}
		if isStopped {
			for _, op := range s.ops {
				if so, ok := op.(node.SchemaNode); ok {
					so.ResetSchema(ctx, s.schemaLayer.GetSchema())
				}
			}
		}
	}
	// Unlock before calling CloseSubTopo to avoid deadlock as CloseSubTopo will lock s again
	s.Unlock()

	if requiresCleanup {
		CloseSubTopo(ctx, s, runId)
	}
}

// RemoveMetrics is called when the rule is deleted
func (s *SrcSubTopo) RemoveMetrics(ruleId string) {
	s.RLock()
	defer s.RUnlock()
	if len(s.refRules) == 0 {
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
	ruleId := "$$subtopo_" + k
	ctx := kctx.WithValue(kctx.RuleBackground(ruleId), kctx.LoggerKey, contextLogger)
	if dParCtx, ok := parCtx.(*kctx.DefaultContext); ok {
		ctx.PropagateTracer(dParCtx)
	}
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
