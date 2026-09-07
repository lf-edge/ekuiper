// Copyright 2022-2026 EMQ Technologies Co., Ltd.
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

package operator

import (
	"encoding/gob"
	"fmt"
	"strings"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
)

const leadOperatorStateKey = "$$lead_operator_state"

type LeadRequestState struct {
	Owner      int
	Remaining  int
	Default    interface{}
	IgnoreNull bool
}

type LeadPendingState struct {
	Row        xsql.Row
	Unresolved int
}

type LeadOperatorState struct {
	Pending       []LeadPendingState
	Calls         map[int]map[string][]LeadRequestState
	LastWatermark time.Time
}

func init() {
	gob.Register(LeadOperatorState{})
}

type leadRequest struct {
	owner      *pendingLeadRow
	origin     xsql.Row
	remaining  int
	dft        interface{}
	ignoreNull bool
}

type leadCallState struct {
	partitions map[string][]*leadRequest
}

type leadDecision struct {
	request   *leadRequest
	resolve   bool
	value     interface{}
	remaining int
}

type pendingLeadRow struct {
	row        xsql.Row
	unresolved int
}

type AnalyticFuncsOp struct {
	Funcs       []*ast.Call
	FieldFuncs  []*ast.Call
	transformed bool

	leadStates    map[int]*leadCallState
	pending       []*pendingLeadRow
	lastWatermark time.Time
}

func setAnalyticValue(input xsql.Row, call *ast.Call, value interface{}) {
	if iv, ok := input.(model.IndexValuer); ok && call.CacheIndex >= 0 {
		iv.SetTempByIndex(call.CacheIndex, value)
	} else {
		input.Set(call.CachedField, value)
	}
}

func (p *AnalyticFuncsOp) evalTupleFunc(calls []*ast.Call, ve *xsql.ValuerEval, input xsql.Row) (xsql.Row, error) {
	for _, call := range calls {
		if call.Name == "lead" {
			continue
		}
		result := ve.Eval(call)
		if e, ok := result.(error); ok {
			return nil, e
		}
		setAnalyticValue(input, call, result)
	}
	return input, nil
}

func (p *AnalyticFuncsOp) evalCollectionFunc(calls []*ast.Call, fv *xsql.FunctionValuer, input xsql.Collection) (xsql.Collection, error) {
	err := input.RangeSet(func(_ int, row xsql.Row) (bool, error) {
		ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(row, &xsql.WindowRangeValuer{WindowRange: input.GetWindowRange()}, fv, &xsql.WildcardValuer{Data: row})}
		for _, call := range calls {
			if call.Name == "lead" {
				return false, fmt.Errorf("lead is not supported on collections")
			}
			result := ve.Eval(call)
			if e, ok := result.(error); ok {
				return false, e
			}
			setAnalyticValue(row, call, result)
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	return input, nil
}

func cloneAnalyticCalls(calls []*ast.Call) []*ast.Call {
	cloned := make([]*ast.Call, len(calls))
	for i, f := range calls {
		cloned[i] = &ast.Call{
			Name:        f.Name,
			FuncId:      f.FuncId,
			FuncType:    f.FuncType,
			Args:        f.Args,
			CachedField: f.CachedField,
			CacheIndex:  f.CacheIndex,
			Partition:   f.Partition,
			WhenExpr:    f.WhenExpr,
			UntilExpr:   f.UntilExpr,
		}
	}
	return cloned
}

func (p *AnalyticFuncsOp) init(ctx api.StreamContext) error {
	if p.transformed {
		return nil
	}
	p.Funcs = cloneAnalyticCalls(p.Funcs)
	p.FieldFuncs = cloneAnalyticCalls(p.FieldFuncs)
	p.leadStates = make(map[int]*leadCallState)
	p.pending = nil
	stored, err := ctx.GetState(leadOperatorStateKey)
	if err != nil {
		return err
	}
	if stored == nil {
		p.transformed = true
		return nil
	}
	snapshot, ok := stored.(LeadOperatorState)
	if !ok {
		return fmt.Errorf("invalid lead operator state %T", stored)
	}
	p.lastWatermark = snapshot.LastWatermark
	owners := make([]*pendingLeadRow, len(snapshot.Pending))
	for i, pending := range snapshot.Pending {
		owners[i] = &pendingLeadRow{row: pending.Row, unresolved: pending.Unresolved}
		p.pending = append(p.pending, owners[i])
	}
	for callID, partitions := range snapshot.Calls {
		callState := &leadCallState{partitions: make(map[string][]*leadRequest)}
		for partition, requests := range partitions {
			for _, request := range requests {
				if request.Owner < 0 || request.Owner >= len(owners) {
					return fmt.Errorf("invalid lead owner index %d", request.Owner)
				}
				owner := owners[request.Owner]
				callState.partitions[partition] = append(callState.partitions[partition], &leadRequest{
					owner: owner, origin: owner.row, remaining: request.Remaining,
					dft: request.Default, ignoreNull: request.IgnoreNull,
				})
			}
		}
		p.leadStates[callID] = callState
	}
	p.transformed = true
	return nil
}

func (p *AnalyticFuncsOp) saveState(ctx api.StreamContext) error {
	ownerIndexes := make(map[*pendingLeadRow]int, len(p.pending))
	snapshot := LeadOperatorState{Calls: make(map[int]map[string][]LeadRequestState), LastWatermark: p.lastWatermark}
	for i, owner := range p.pending {
		ownerIndexes[owner] = i
		snapshot.Pending = append(snapshot.Pending, LeadPendingState{Row: owner.row, Unresolved: owner.unresolved})
	}
	for callID, state := range p.leadStates {
		partitions := make(map[string][]LeadRequestState)
		for partition, requests := range state.partitions {
			for _, request := range requests {
				owner, ok := ownerIndexes[request.owner]
				if !ok {
					return fmt.Errorf("lead request references an emitted row")
				}
				partitions[partition] = append(partitions[partition], LeadRequestState{
					Owner: owner, Remaining: request.remaining, Default: request.dft, IgnoreNull: request.ignoreNull,
				})
			}
		}
		snapshot.Calls[callID] = partitions
	}
	return ctx.PutState(leadOperatorStateKey, snapshot)
}

func (p *AnalyticFuncsOp) leadCalls() []*ast.Call {
	seen := make(map[int]bool)
	var result []*ast.Call
	for _, calls := range [][]*ast.Call{p.FieldFuncs, p.Funcs} {
		for _, call := range calls {
			if call.Name == "lead" && !seen[call.FuncId] {
				seen[call.FuncId] = true
				result = append(result, call)
			}
		}
	}
	return result
}

func evalBool(ve *xsql.ValuerEval, expr ast.Expr, clause string) (bool, error) {
	if expr == nil {
		return false, nil
	}
	value := ve.Eval(expr)
	if err, ok := value.(error); ok {
		return false, err
	}
	if value == nil {
		return false, nil
	}
	result, ok := value.(bool)
	if !ok {
		return false, fmt.Errorf("lead %s expression must return boolean but got %T", clause, value)
	}
	return result, nil
}

func evalPartitionKey(call *ast.Call, ve *xsql.ValuerEval) (string, error) {
	if call.Partition == nil || len(call.Partition.Exprs) == 0 {
		return "self", nil
	}
	var key strings.Builder
	for _, expr := range call.Partition.Exprs {
		value := ve.Eval(expr)
		if err, ok := value.(error); ok {
			return "", err
		}
		fmt.Fprintf(&key, "%T:%v;", value, value)
	}
	return key.String(), nil
}

func leadOptions(call *ast.Call, originEval *xsql.ValuerEval) (offset int, dft interface{}, ignoreNull bool, err error) {
	offset = 1
	ignoreNull = true
	if len(call.Args) >= 2 {
		offset = int(call.Args[1].(*ast.IntegerLiteral).Val)
	}
	if len(call.Args) >= 3 {
		dft = originEval.Eval(call.Args[2])
		if e, ok := dft.(error); ok {
			return 0, nil, false, e
		}
	}
	if len(call.Args) == 4 {
		value := originEval.Eval(call.Args[3])
		if e, ok := value.(error); ok {
			return 0, nil, false, e
		}
		var ok bool
		ignoreNull, ok = value.(bool)
		if !ok {
			return 0, nil, false, fmt.Errorf("the fourth arg of lead must return boolean but got %T", value)
		}
	}
	return offset, dft, ignoreNull, nil
}

// prepareLeadCall validates a probe without changing pending requests. All calls
// for a row must prepare successfully before any of their decisions are committed.
func (p *AnalyticFuncsOp) prepareLeadCall(call *ast.Call, probe xsql.Row, owner *pendingLeadRow, fv *xsql.FunctionValuer) (func(), error) {
	probeValuer := xsql.MultiValuer(probe, fv)
	probeEval := &xsql.ValuerEval{Valuer: probeValuer}
	partition, err := evalPartitionKey(call, probeEval)
	if err != nil {
		return nil, err
	}
	state, ok := p.leadStates[call.FuncId]
	if !ok {
		state = &leadCallState{partitions: make(map[string][]*leadRequest)}
	}

	waiting := state.partitions[partition]
	decisions := make([]leadDecision, 0, len(waiting))
	for _, request := range waiting {
		expired := false
		if call.UntilExpr != nil {
			untilValuer := xsql.NewLeadUntilValuer(probeValuer, xsql.MultiValuer(request.origin, fv))
			expired, err = evalBool(&xsql.ValuerEval{Valuer: untilValuer}, call.UntilExpr, "UNTIL")
			if err != nil {
				return nil, err
			}
		}
		if expired {
			decisions = append(decisions, leadDecision{request: request, resolve: true, value: request.dft})
			continue
		}
		decisions = append(decisions, leadDecision{request: request, remaining: request.remaining})
	}

	// An expired request must not evaluate WHEN or the candidate expression.
	var candidate interface{}
	when, evaluated := true, false
	for i := range decisions {
		decision := &decisions[i]
		if decision.resolve {
			continue
		}
		if !evaluated {
			if call.WhenExpr != nil {
				when, err = evalBool(probeEval, call.WhenExpr, "WHEN")
				if err != nil {
					return nil, err
				}
			}
			if when {
				candidate = probeEval.Eval(call.Args[0])
				if e, ok := candidate.(error); ok {
					return nil, e
				}
			}
			evaluated = true
		}
		request := decision.request
		if when && (!request.ignoreNull || candidate != nil) {
			remaining := request.remaining - 1
			if remaining == 0 {
				*decision = leadDecision{request: request, resolve: true, value: candidate}
				continue
			}
			decision.remaining = remaining
		}
	}

	offset, dft, ignoreNull, err := leadOptions(call, probeEval)
	if err != nil {
		return nil, err
	}
	return func() {
		p.leadStates[call.FuncId] = state
		kept := make([]*leadRequest, 0, len(waiting)+1)
		for _, decision := range decisions {
			if decision.resolve {
				setAnalyticValue(decision.request.owner.row, call, decision.value)
				decision.request.owner.unresolved--
				continue
			}
			decision.request.remaining = decision.remaining
			kept = append(kept, decision.request)
		}
		state.partitions[partition] = append(kept, &leadRequest{
			owner:      owner,
			origin:     probe,
			remaining:  offset,
			dft:        dft,
			ignoreNull: ignoreNull,
		})
	}, nil
}

// Watermark holds downstream event time behind every buffered row. The next
// upstream watermark can advance it after those rows have been emitted.
func (p *AnalyticFuncsOp) Watermark(ctx api.StreamContext, marker *xsql.WatermarkTuple) (*xsql.WatermarkTuple, error) {
	if err := p.init(ctx); err != nil {
		return nil, err
	}
	if len(p.leadCalls()) == 0 {
		return marker, nil
	}
	ts := marker.Timestamp
	for _, pending := range p.pending {
		event, ok := pending.row.(xsql.Event)
		if !ok {
			// A row without an event timestamp cannot establish a safe watermark.
			return nil, nil
		}
		limit := event.GetTimestamp().Add(-time.Nanosecond)
		if limit.Before(ts) {
			ts = limit
		}
	}
	if !p.lastWatermark.IsZero() && !ts.After(p.lastWatermark) {
		return nil, nil
	}
	p.lastWatermark = ts
	if err := p.saveState(ctx); err != nil {
		return nil, err
	}
	return &xsql.WatermarkTuple{Timestamp: ts}, nil
}

func (p *AnalyticFuncsOp) emitReady() []xsql.Row {
	var ready []xsql.Row
	for len(p.pending) > 0 && p.pending[0].unresolved == 0 {
		ready = append(ready, p.pending[0].row)
		p.pending[0] = nil
		p.pending = p.pending[1:]
	}
	return ready
}

// Finalize resolves the unresolved tail with each request's default before EOF.
func (p *AnalyticFuncsOp) Finalize(ctx api.StreamContext, _ interface{}, _ *xsql.FunctionValuer, _ *xsql.AggregateFunctionValuer) interface{} {
	if err := p.init(ctx); err != nil {
		return err
	}
	for _, call := range p.leadCalls() {
		state := p.leadStates[call.FuncId]
		if state == nil {
			continue
		}
		for partition, requests := range state.partitions {
			for _, request := range requests {
				setAnalyticValue(request.owner.row, call, request.dft)
				request.owner.unresolved--
			}
			delete(state.partitions, partition)
		}
	}
	for _, owner := range p.pending {
		if owner.unresolved != 0 {
			return fmt.Errorf("lead finalize found %d unresolved calls", owner.unresolved)
		}
	}
	ready := p.emitReady()
	p.lastWatermark = time.Time{}
	if err := ctx.DeleteState(leadOperatorStateKey); err != nil {
		return err
	}
	return ready
}

func (p *AnalyticFuncsOp) applyRow(ctx api.StreamContext, input xsql.Row, fv *xsql.FunctionValuer) interface{} {
	ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(input, fv)}
	var err error
	input, err = p.evalTupleFunc(p.FieldFuncs, ve, input)
	if err != nil {
		return err
	}
	input, err = p.evalTupleFunc(p.Funcs, ve, input)
	if err != nil {
		return err
	}

	leads := p.leadCalls()
	if len(leads) == 0 {
		return input
	}
	owner := &pendingLeadRow{row: input, unresolved: len(leads)}
	commits := make([]func(), 0, len(leads))
	for _, call := range leads {
		commit, err := p.prepareLeadCall(call, input, owner, fv)
		if err != nil {
			return err
		}
		commits = append(commits, commit)
	}
	p.pending = append(p.pending, owner)
	for _, commit := range commits {
		commit()
	}
	ready := p.emitReady()
	if err := p.saveState(ctx); err != nil {
		return err
	}
	if len(ready) == 0 {
		return nil
	}
	return ready
}

func (p *AnalyticFuncsOp) Apply(ctx api.StreamContext, data interface{}, fv *xsql.FunctionValuer, _ *xsql.AggregateFunctionValuer) interface{} {
	ctx.GetLogger().Debugf("AnalyticFuncsOp receive: %v", data)
	if err := p.init(ctx); err != nil {
		return err
	}
	switch input := data.(type) {
	case error:
		return input
	case xsql.Row:
		return p.applyRow(ctx, input, fv)
	case xsql.Collection:
		var err error
		input, err = p.evalCollectionFunc(p.FieldFuncs, fv, input)
		if err != nil {
			return err
		}
		input, err = p.evalCollectionFunc(p.Funcs, fv, input)
		if err != nil {
			return err
		}
		return input
	default:
		return fmt.Errorf("run analytic funcs op error: invalid input %[1]T(%[1]v)", input)
	}
}
