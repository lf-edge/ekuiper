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
	"fmt"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

// leadBuffer owns delayed rows and their requests. Its invariants are:
//   - Every outstanding request belongs to a row in pending.
//   - A row's unresolved count equals its outstanding request count.
//   - Only a completed prefix of pending can be emitted.
type leadBuffer struct {
	calls         []*ast.Call
	requests      map[int]map[string][]*leadRequest
	pending       []*pendingLeadRow
	lastWatermark time.Time
}

type pendingLeadRow struct {
	row        xsql.Row
	unresolved int
}

type leadRequest struct {
	owner      *pendingLeadRow
	remaining  int
	dft        interface{}
	ignoreNull bool
}

func (r *leadRequest) complete(call *ast.Call, value interface{}) {
	setAnalyticValue(r.owner.row, call, value)
	r.owner.unresolved--
}

// leadDecision is a proposed update; remaining == 0 means the request completes.
// Its value may be nil, which is also a valid completed result.
type leadDecision struct {
	request   *leadRequest
	remaining int
	value     interface{}
}

// leadCallUpdate contains all changes for one call and one incoming probe.
// Preparing it does not change the request queues or their owners.
type leadCallUpdate struct {
	call      *ast.Call
	partition string
	decisions []leadDecision
	next      *leadRequest
}

func (b *leadBuffer) apply(row xsql.Row, fv *xsql.FunctionValuer) interface{} {
	owner := &pendingLeadRow{row: row, unresolved: len(b.calls)}
	updates, err := b.prepare(owner, fv)
	if err != nil {
		return err
	}
	b.commit(owner, updates)
	ready := b.drainReady()
	if len(ready) == 0 {
		return nil
	}
	return ready
}

// All LEAD calls must prepare successfully before their request updates commit.
// This does not roll back other stateful functions evaluated on the input row.
func (b *leadBuffer) prepare(owner *pendingLeadRow, fv *xsql.FunctionValuer) ([]leadCallUpdate, error) {
	updates := make([]leadCallUpdate, 0, len(b.calls))
	for _, call := range b.calls {
		update, err := b.prepareCall(call, owner, fv)
		if err != nil {
			return nil, err
		}
		updates = append(updates, update)
	}
	return updates, nil
}

func (b *leadBuffer) prepareCall(call *ast.Call, owner *pendingLeadRow, fv *xsql.FunctionValuer) (leadCallUpdate, error) {
	probeValuer := xsql.MultiValuer(owner.row, fv)
	probeEval := &xsql.ValuerEval{Valuer: probeValuer}
	partition, err := evalPartitionKey(call, probeEval)
	if err != nil {
		return leadCallUpdate{}, err
	}
	update := leadCallUpdate{call: call, partition: partition}
	needsCandidate := false
	for _, request := range b.requests[call.FuncId][partition] {
		untilValuer := xsql.NewLeadUntilValuer(probeValuer, xsql.MultiValuer(request.owner.row, fv))
		expired, err := evalBool(&xsql.ValuerEval{Valuer: untilValuer}, call.UntilExpr, "UNTIL")
		if err != nil {
			return leadCallUpdate{}, err
		}
		decision := leadDecision{request: request, remaining: request.remaining}
		if expired {
			decision.remaining = 0
			decision.value = request.dft
		} else {
			needsCandidate = true
		}
		update.decisions = append(update.decisions, decision)
	}
	if needsCandidate {
		candidate, matches, err := evalLeadCandidate(call, probeEval)
		if err != nil {
			return leadCallUpdate{}, err
		}
		if matches {
			for i := range update.decisions {
				decision := &update.decisions[i]
				if decision.remaining == 0 || (decision.request.ignoreNull && candidate == nil) {
					continue
				}
				decision.remaining--
				if decision.remaining == 0 {
					decision.value = candidate
				}
			}
		}
	}
	offset, dft, ignoreNull, err := leadOptions(call, probeEval)
	if err != nil {
		return leadCallUpdate{}, err
	}
	update.next = &leadRequest{owner: owner, remaining: offset, dft: dft, ignoreNull: ignoreNull}
	return update, nil
}

func evalLeadCandidate(call *ast.Call, ve *xsql.ValuerEval) (interface{}, bool, error) {
	if call.WhenExpr != nil {
		matches, err := evalBool(ve, call.WhenExpr, "WHEN")
		if err != nil || !matches {
			return nil, false, err
		}
	}
	value := ve.Eval(call.Args[0])
	if err, ok := value.(error); ok {
		return nil, false, err
	}
	return value, true, nil
}

func (b *leadBuffer) commit(owner *pendingLeadRow, updates []leadCallUpdate) {
	b.pending = append(b.pending, owner)
	for _, update := range updates {
		kept := make([]*leadRequest, 0, len(update.decisions)+1)
		for _, decision := range update.decisions {
			if decision.remaining == 0 {
				decision.request.complete(update.call, decision.value)
			} else {
				decision.request.remaining = decision.remaining
				kept = append(kept, decision.request)
			}
		}
		partitions := b.requests[update.call.FuncId]
		if partitions == nil {
			partitions = make(map[string][]*leadRequest)
			b.requests[update.call.FuncId] = partitions
		}
		// A row only starts looking ahead after serving as a probe for older rows.
		partitions[update.partition] = append(kept, update.next)
	}
}

func (b *leadBuffer) drainReady() []xsql.Row {
	var ready []xsql.Row
	for len(b.pending) > 0 && b.pending[0].unresolved == 0 {
		ready = append(ready, b.pending[0].row)
		b.pending[0] = nil
		b.pending = b.pending[1:]
	}
	return ready
}

// finalize resolves the unresolved tail before the node forwards EOF.
func (b *leadBuffer) finalize(ctx api.StreamContext) interface{} {
	for _, call := range b.calls {
		for partition, requests := range b.requests[call.FuncId] {
			for _, request := range requests {
				request.complete(call, request.dft)
			}
			delete(b.requests[call.FuncId], partition)
		}
	}
	for _, owner := range b.pending {
		if owner.unresolved != 0 {
			return fmt.Errorf("lead finalize found %d unresolved calls", owner.unresolved)
		}
	}
	ready := b.drainReady()
	b.lastWatermark = time.Time{}
	if err := ctx.DeleteState(leadOperatorStateKey); err != nil {
		return err
	}
	return ready
}

// Watermark holds downstream event time behind every buffered row. The next
// upstream watermark can advance it after those rows have been emitted.
func (b *leadBuffer) watermark(marker *xsql.WatermarkTuple) (*xsql.WatermarkTuple, error) {
	ts := marker.Timestamp
	for _, pending := range b.pending {
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
	if !b.lastWatermark.IsZero() && !ts.After(b.lastWatermark) {
		return nil, nil
	}
	b.lastWatermark = ts
	return &xsql.WatermarkTuple{Timestamp: ts}, nil
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
	values := make([]any, 0, len(call.Partition.Exprs))
	for _, expr := range call.Partition.Exprs {
		value := ve.Eval(expr)
		if err, ok := value.(error); ok {
			return "", err
		}
		values = append(values, value)
	}
	return xsql.EncodePartitionKey(values...), nil
}

func leadOptions(call *ast.Call, originEval *xsql.ValuerEval) (offset int, dft interface{}, ignoreNull bool, err error) {
	offset = 1
	ignoreNull = true
	if len(call.Args) >= 2 {
		literal, ok := call.Args[1].(*ast.IntegerLiteral)
		if !ok || literal.Val <= 0 || uint64(literal.Val) > uint64(^uint(0)>>1) {
			return 0, nil, false, fmt.Errorf("the second arg of lead must be a positive integer literal within the supported range")
		}
		offset = int(literal.Val)
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
