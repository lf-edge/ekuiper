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
	"github.com/lf-edge/ekuiper/v2/internal/topo/schema"
	"github.com/lf-edge/ekuiper/v2/internal/topo/state"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
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
	sync.RWMutex
	refRules map[string]map[int]chan<- error // map[ruleId][runId]errCh, notify the rule for errors
	// Runtime state, affect the running loop. Update when any rule opened or all rules stopped
	opened           atomic.Int32 // 0 is init, 1 is open, -1 is close
	cancel           context.CancelFunc
	enableCheckpoint bool
}

const (
	InitState  int32 = 0
	OpenState  int32 = 1
	CloseState int32 = -1
)

func (s *SrcSubTopo) Init(ctx api.StreamContext) {
	s.Lock()
	defer s.Unlock()
	s.updateRef(ctx, nil)
}

// Open is different from main topo because this will run multiple times.
// Each new ref rule will run open subtopo
func (s *SrcSubTopo) Open(ctx api.StreamContext, parentErrCh chan<- error) {
	s.Lock()
	defer s.Unlock()
	// Update the ref count
	rl := s.updateRef(ctx, parentErrCh)
	if !rl {
		ctx.GetLogger().Infof("subtopo %s already close, so ignore open", s.name)
		return
	}
	// Attach schemas
	err := s.schemaLayer.Attach(ctx)
	if err != nil {
		ctx.GetLogger().Warnf("attach schema layer failed: %s", err)
	} else {
		for _, op := range s.ops {
			if so, ok := op.(node.SchemaNode); ok {
				ctx.GetLogger().Infof("reset schema to op %s", op.GetName())
				so.ResetSchema(ctx, s.schemaLayer.GetSchema())
			}
		}
	}
	// If not opened yet, open it. Each ref rule start will try to run this.
	if s.opened.CompareAndSwap(InitState, OpenState) {
		poe := infra.SafeRun(func() error {
			ctx.GetLogger().Infof("Opening subtopo %s by rule %s", s.name, ctx.GetRuleId())
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
			ctx.GetLogger().Infof("subtopo %s opened by rule %s with 1 ref", s.name, ctx.GetRuleId())
			go func() (e error) {
				defer func() {
					s.Lock()
					s.opened.Store(CloseState)
					conf.Log.Infof("subtopo %s closed", s.name)
					if e != nil {
						s.notifyError(e)
					}
					s.Unlock()
				}()
				for {
					select {
					case e = <-errCh:
						pctx.GetLogger().Infof("subtopo %s exit for error %v", s.name, e)
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
	} else {
		ctx.GetLogger().Infof("subtopo %s already started by other rule", s.name)
	}
}

// Close is different from main topo because this will run multiple times.
// Close ref rule will run close subtopo
func (s *SrcSubTopo) Close(ctx api.StreamContext) {
	s.Lock()
	defer s.Unlock()
	isStop, isDestroy := s.removeRef(ctx)
	if isDestroy { // destroy this subtopo
		if s.cancel != nil {
			s.cancel()
		}
		subCtx := ctx.(*kctx.DefaultContext).WithRuleId(fmt.Sprintf("$$subtopo_%s", s.name)).WithRun(0)
		if ss, ok := s.source.(*SrcSubTopo); ok {
			ss.Close(subCtx)
		}
		ctx.GetLogger().Infof("subtopo %s removed", s.name)
	} else {
		ctx.GetLogger().Infof("subtopo %s update schema for rule %s change", s.name, ctx.GetRuleId())
		err := s.schemaLayer.Detach(ctx, isStop)
		if err != nil {
			ctx.GetLogger().Warnf("detach schema layer failed: %s", err)
		} else {
			for _, op := range s.ops {
				if so, ok := op.(node.SchemaNode); ok {
					so.ResetSchema(ctx, s.schemaLayer.GetSchema())
				}
			}
		}
	}
	_ = s.RemoveOutput(fmt.Sprintf("%s.%d", ctx.GetRuleId(), ctx.GetRunId()))
}

// IsSliceMode this is a constant set when creating new subtopo
func (s *SrcSubTopo) IsSliceMode() bool {
	return s.isSliceMode
}

func (s *SrcSubTopo) AddOutput(output chan interface{}, name string) error {
	return s.tail.AddOutput(output, name)
}

func (s *SrcSubTopo) RemoveOutput(name string) error {
	return s.tail.RemoveOutput(name)
}

func (s *SrcSubTopo) updateRef(ctx api.StreamContext, parentErrCh chan<- error) bool {
	if s.opened.Load() == CloseState {
		return false
	}
	rr, ok := s.refRules[ctx.GetRuleId()]
	if !ok {
		rr = map[int]chan<- error{}
		s.refRules[ctx.GetRuleId()] = rr
		ctx.GetLogger().Infof("subtopo %s add rule ref: %s, count: %d", s.name, ctx.GetRuleId(), len(s.refRules))
	}
	_, hasRun := rr[ctx.GetRunId()]
	if hasRun {
		if parentErrCh != nil {
			ctx.GetLogger().Infof("subtopo %s for rule %s replaced, count: %d", s.name, ctx.GetRuleId(), len(s.refRules))
		} else {
			ctx.GetLogger().Warnf("subtopo %s for rule %s reset, count: %d", s.name, ctx.GetRuleId(), len(s.refRules))
		}
	} else {
		if parentErrCh != nil {
			ctx.GetLogger().Infof("subtopo %s for rule %s opened, count: %d", s.name, ctx.GetRuleId(), len(s.refRules))
		} else {
			ctx.GetLogger().Infof("subtopo %s for rule %s init, count: %d", s.name, ctx.GetRuleId(), len(s.refRules))
		}
	}
	rr[ctx.GetRunId()] = parentErrCh
	return true
}

func (s *SrcSubTopo) removeRef(ctx api.StreamContext) (ruleClose bool, destroy bool) {
	rr, ok := s.refRules[ctx.GetRuleId()]
	if ok { // Run inside ok to make sure clean up only run once
		delete(rr, ctx.GetRunId())
		if len(rr) == 0 {
			delete(s.refRules, ctx.GetRuleId())
			ruleClose = true
			ctx.GetLogger().Infof("subtopo %s for rule %s closed, count: %d", s.name, ctx.GetRuleId(), len(s.refRules))
		}
		if len(s.refRules) == 0 {
			RemoveSubTopo(s.name)
			destroy = true
			s.opened.Store(CloseState)
			ctx.GetLogger().Infof("subtopo %s closed", s.name)
		}
	}
	return
}

func (s *SrcSubTopo) notifyError(poe error) {
	// Notify error to all ref rules
	for k, rr := range s.refRules {
		conf.Log.Debugf("Notify error %v to rule %s", poe, k)
		for _, e := range rr {
			infra.DrainError(nil, poe, e)
		}
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
	return sctx.WithMeta(ruleId, opId, store).WithRun(0), cancel, nil
}

var (
	_ node.DataSourceNode          = &SrcSubTopo{}
	_ checkpoint.SourceSubTopoTask = &SrcSubTopo{}
)
