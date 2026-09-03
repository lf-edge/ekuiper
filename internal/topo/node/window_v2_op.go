// Copyright 2025-2026 EMQ Technologies Co., Ltd.
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

package node

import (
	"encoding/gob"
	"fmt"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

const (
	V2WindowInputsKey = "$$v2windowInputs"
)

var InfTime = time.Unix(1<<63-62135596801, 999999999)

func init() {
	gob.Register([]*xsql.Tuple{})
	gob.Register(&WindowScanner{})
	gob.Register(time.Time{})
	gob.Register(&StateWindowStatus{})
	gob.Register(map[string]*StateWindowStatus{})
	gob.Register(&SlidingWindowV2State{})
	gob.Register(&EventSlidingWindowV2State{})
}

type WindowV2Operator struct {
	*defaultSinkNode
	windowConfig WindowConfig
	wExec        WindowV2Exec
	scanner      *WindowScanner
}

func NewWindowV2Op(name string, w WindowConfig, options *def.RuleOption) (*WindowV2Operator, error) {
	o := new(WindowV2Operator)
	o.defaultSinkNode = newDefaultSinkNode(name, options)
	o.scanner = &WindowScanner{Tuples: make([]*xsql.Tuple, 0)}
	o.windowConfig = w
	switch w.Type {
	case ast.SLIDING_WINDOW:
		if options.IsEventTime {
			o.wExec = NewEventSlidingWindowOp(o)
		} else {
			o.wExec = NewSlidingWindowOp(o)
		}
	case ast.STATE_WINDOW:
		o.wExec = NewStateWindowOp(o)
	default:
		return nil, fmt.Errorf("unsupported window type:%v", w.Type.String())
	}
	return o, nil
}

func (o *WindowV2Operator) Close() {
	o.defaultNode.Close()
}

func (o *WindowV2Operator) Exec(ctx api.StreamContext, errCh chan<- error) {
	o.prepareExec(ctx, errCh, "op")
	go func() {
		defer o.Close()
		err := infra.SafeRun(func() error {
			o.wExec.exec(ctx, errCh)
			return nil
		})
		if err != nil {
			infra.DrainError(ctx, err, errCh)
		}
	}()
}

func (o *WindowV2Operator) emitWindow(ctx api.StreamContext, startTime, endTime time.Time) {
	tuples := o.scanner.scanWindow(startTime, endTime)
	results := &xsql.WindowTuples{
		Content: make([]xsql.Row, 0),
	}
	for _, tuple := range tuples {
		results.Content = append(results.Content, tuple)
	}
	results.WindowRange = xsql.NewWindowRange(startTime.UnixMilli(), endTime.UnixMilli(), endTime.UnixMilli())
	o.Broadcast(results)
	o.onSend(ctx, results)
}

type WindowV2Exec interface {
	exec(ctx api.StreamContext, errCh chan<- error)
}

type StateWindowOp struct {
	*WindowV2Operator
	status          map[string]*StateWindowStatus
	PartitionExpr   *ast.PartitionExpr
	SingleCondition ast.Expr
	BeginCondition  ast.Expr
	EmitCondition   ast.Expr
	stateFuncs      []*ast.Call
}

type StateWindowStatus struct {
	StartTime time.Time
	EndTime   time.Time
	OnBegin   bool
	Scanner   *WindowScanner
}

func NewStateWindowOp(o *WindowV2Operator) *StateWindowOp {
	return &StateWindowOp{
		WindowV2Operator: o,
		BeginCondition:   o.windowConfig.BeginCondition,
		EmitCondition:    o.windowConfig.EmitCondition,
		SingleCondition:  o.windowConfig.SingleCondition,
		stateFuncs:       o.windowConfig.StateFuncs,
		status:           make(map[string]*StateWindowStatus),
		PartitionExpr:    o.windowConfig.PartitionExpr,
	}
}

func (s *StateWindowOp) emit(ctx api.StreamContext, status *StateWindowStatus) {
	tuples := status.Scanner.scanWindow(time.Time{}, InfTime)
	results := &xsql.WindowTuples{
		Content: make([]xsql.Row, 0),
	}
	for _, tuple := range tuples {
		results.Content = append(results.Content, tuple)
	}
	results.WindowRange = xsql.NewWindowRange(status.StartTime.UnixMilli(), status.EndTime.UnixMilli(), status.EndTime.UnixMilli())
	s.Broadcast(results)
	s.onSend(ctx, results)
}

func calPartition(fv *xsql.FunctionValuer, partitionExpr *ast.PartitionExpr, row *xsql.Tuple) string {
	name := "parKey_"
	if partitionExpr == nil {
		return name
	}
	ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(row, fv, &xsql.WildcardValuer{Data: row})}
	for _, expr := range partitionExpr.Exprs {
		r := ve.Eval(expr)
		if _, ok := r.(error); ok {
			continue
		} else {
			name += fmt.Sprintf("%v,", r)
		}
	}
	return name
}

func (s *StateWindowOp) exec(ctx api.StreamContext, errCh chan<- error) {
	v, err := ctx.GetState(V2WindowInputsKey)
	if err != nil {
		infra.DrainError(ctx, err, errCh)
		return
	}
	if v != nil {
		preStatus, ok := v.(map[string]*StateWindowStatus)
		if !ok {
			infra.DrainError(ctx, fmt.Errorf("restore window V2 state %T error, invalid type", v), errCh)
			return
		}
		s.status = preStatus
	}
	if err := ctx.PutState(V2WindowInputsKey, s.status); err != nil {
		infra.DrainError(ctx, err, errCh)
		return
	}
	fv, _ := xsql.NewFunctionValuersForOp(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case input := <-s.input:
			data, processed := s.commonIngest(ctx, input)
			if processed {
				continue
			}
			s.onProcessStart(ctx, input)
			switch row := data.(type) {
			case *xsql.Tuple:
				name := calPartition(fv, s.PartitionExpr, row)
				status, ok := s.status[name]
				if !ok {
					status = &StateWindowStatus{
						Scanner: &WindowScanner{Tuples: make([]*xsql.Tuple, 0)},
					}
					s.status[name] = status
				}
				if s.BeginCondition != nil && s.EmitCondition != nil {
					s.handleTupleWithBeginEmitCondition(ctx, fv, row, status)
				} else if s.SingleCondition != nil {
					s.handleTupleWithSingleCondition(ctx, fv, row, status)
				}
			}
			s.onProcessEnd(ctx)
		}
	}
}

func (s *StateWindowOp) handleTupleWithBeginEmitCondition(ctx api.StreamContext, fv *xsql.FunctionValuer, row *xsql.Tuple, status *StateWindowStatus) {
	if !status.OnBegin {
		canBegin := isMatchCondition(ctx, s.BeginCondition, fv, row, s.stateFuncs)
		if canBegin {
			status.StartTime = row.Timestamp
			status.OnBegin = true
			status.Scanner.addTuple(row)
		}
	} else {
		status.Scanner.addTuple(row)
		canEmit := isMatchCondition(ctx, s.EmitCondition, fv, row, s.stateFuncs)
		if canEmit {
			status.EndTime = row.Timestamp
			s.emit(ctx, status)
			status.Scanner.gc(InfTime)
			status.OnBegin = false
		}
	}
}

func (s *StateWindowOp) handleTupleWithSingleCondition(ctx api.StreamContext, fv *xsql.FunctionValuer, row *xsql.Tuple, status *StateWindowStatus) {
	if !status.OnBegin {
		canBegin := isMatchCondition(ctx, s.SingleCondition, fv, row, s.stateFuncs)
		if canBegin {
			status.StartTime = row.Timestamp
			status.OnBegin = true
			status.Scanner.addTuple(row)
		}
	} else {
		canEmit := isMatchCondition(ctx, s.SingleCondition, fv, row, s.stateFuncs)
		if canEmit {
			status.EndTime = row.Timestamp
			s.emit(ctx, status)
			status.Scanner.gc(InfTime)
			status.OnBegin = true
			status.Scanner.addTuple(row)
			status.StartTime = row.Timestamp
		} else {
			status.Scanner.addTuple(row)
		}
	}
}

type SlidingWindowOp struct {
	*WindowV2Operator
	Delay            time.Duration
	Length           time.Duration
	stateFuncs       []*ast.Call
	triggerCondition ast.Expr
	state            *SlidingWindowV2State
	delayNotify      chan uint64
}

// SlidingWindowV2State is published once and then owned by the operator
// goroutine. The checkpoint barrier performs the only deep freeze.
type SlidingWindowV2State struct {
	Scanner *WindowScanner
	// Pending is ordered by trigger time. WindowScanner already requires
	// timestamp-ordered input, so the first item also has the earliest
	// window start needed by GC.
	Pending           []PendingSlidingWindow
	NextPendingID     uint64
	LatestWindowStart time.Time
}

type PendingSlidingWindow struct {
	ID uint64
	// FireAt is an absolute processing-time deadline, so downtime counts
	// toward Delay and an overdue window fires immediately after restore.
	FireAt time.Time
	// WindowEnd preserves the tuple-timestamp-based range used by the
	// original execution, independently of the processing clock.
	WindowEnd time.Time
}

func NewSlidingWindowOp(o *WindowV2Operator) *SlidingWindowOp {
	return &SlidingWindowOp{
		WindowV2Operator: o,
		Delay:            o.windowConfig.Delay,
		Length:           o.windowConfig.Length,
		stateFuncs:       o.windowConfig.StateFuncs,
		triggerCondition: o.windowConfig.TriggerCondition,
		state: &SlidingWindowV2State{
			Scanner: o.scanner,
			Pending: make([]PendingSlidingWindow, 0),
		},
		delayNotify: make(chan uint64, 1024),
	}
}

func (s *SlidingWindowOp) exec(ctx api.StreamContext, errCh chan<- error) {
	if err := s.restoreState(ctx); err != nil {
		infra.DrainError(ctx, err, errCh)
		return
	}
	fv, _ := xsql.NewFunctionValuersForOp(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case pendingID := <-s.delayNotify:
			s.emitPendingWindow(ctx, pendingID)
		case input := <-s.input:
			data, processed := s.commonIngest(ctx, input)
			if processed {
				continue
			}
			s.onProcessStart(ctx, input)
			switch row := data.(type) {
			case *xsql.Tuple:
				windowEnd := row.Timestamp
				windowStart := windowEnd.Add(-s.Length)
				s.state.LatestWindowStart = windowStart
				s.gcScanner()
				s.scanner.addTuple(row)
				sendWindow := true
				if s.triggerCondition != nil {
					sendWindow = isMatchCondition(ctx, s.triggerCondition, fv, row, s.stateFuncs)
				}
				if s.Delay > 0 && sendWindow {
					sendWindow = false
					pending := PendingSlidingWindow{
						ID:        s.state.NextPendingID,
						FireAt:    timex.GetNow().Add(s.Delay),
						WindowEnd: windowEnd.Add(s.Delay),
					}
					s.state.NextPendingID++
					s.state.Pending = append(s.state.Pending, pending)
					s.schedulePendingWindow(ctx, pending)
				}
				if sendWindow {
					s.emitWindow(ctx, windowStart, windowEnd)
				}
			}
			s.onProcessEnd(ctx)
		}
	}
}

func (s *SlidingWindowOp) restoreState(ctx api.StreamContext) error {
	v, err := ctx.GetState(V2WindowInputsKey)
	if err != nil {
		return err
	}
	if v != nil {
		state, ok := v.(*SlidingWindowV2State)
		if !ok {
			return fmt.Errorf("restore window V2 sliding state %T error, invalid type", v)
		}
		if state.Scanner == nil {
			state.Scanner = &WindowScanner{Tuples: make([]*xsql.Tuple, 0)}
		}
		s.state = state
		s.scanner = state.Scanner
	}
	now := timex.GetNow()
	pending := append([]PendingSlidingWindow(nil), s.state.Pending...)
	for _, item := range pending {
		if item.FireAt.After(now) {
			s.schedulePendingWindow(ctx, item)
		} else {
			s.emitPendingWindow(ctx, item.ID)
		}
	}
	return ctx.PutState(V2WindowInputsKey, s.state)
}

func (s *SlidingWindowOp) schedulePendingWindow(ctx api.StreamContext, pending PendingSlidingWindow) {
	after := timex.After(pending.FireAt.Sub(timex.GetNow()))
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-after:
			select {
			case <-ctx.Done():
			case s.delayNotify <- pending.ID:
			}
		}
	}()
}

func (s *SlidingWindowOp) emitPendingWindow(ctx api.StreamContext, pendingID uint64) {
	if len(s.state.Pending) > 0 && s.state.Pending[0].ID == pendingID {
		pending := s.state.Pending[0]
		s.state.Pending = s.state.Pending[1:]
		s.emitPending(ctx, pending)
		return
	}
	for i, pending := range s.state.Pending {
		if pending.ID != pendingID {
			continue
		}
		s.state.Pending = append(s.state.Pending[:i], s.state.Pending[i+1:]...)
		s.emitPending(ctx, pending)
		break
	}
}

func (s *SlidingWindowOp) emitPending(ctx api.StreamContext, pending PendingSlidingWindow) {
	windowStart := pending.WindowEnd.Add(-s.Delay).Add(-s.Length)
	s.emitWindow(ctx, windowStart, pending.WindowEnd)
	s.gcScanner()
}

func (s *SlidingWindowOp) gcScanner() {
	gcTime := s.state.LatestWindowStart
	if gcTime.IsZero() {
		return
	}
	if len(s.state.Pending) > 0 {
		pendingStart := s.state.Pending[0].WindowEnd.Add(-s.Delay).Add(-s.Length)
		if pendingStart.Before(gcTime) {
			gcTime = pendingStart
		}
	}
	s.scanner.gc(gcTime)
}

func isMatchCondition(ctx api.StreamContext, condition ast.Expr, fv *xsql.FunctionValuer, d *xsql.Tuple, stateFuncs []*ast.Call) bool {
	if condition == nil {
		return true
	}
	log := ctx.GetLogger()
	ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(d, fv)}
	result := ve.Eval(condition)
	// not match trigger condition
	if result == nil {
		return false
	}
	switch v := result.(type) {
	case error:
		log.Errorf("inc sliding window trigger condition meet error: %v", v)
		return false
	case bool:
		if v && len(stateFuncs) > 0 {
			for _, f := range stateFuncs {
				_ = ve.Eval(f)
			}
		}
		return v
	default:
		return false
	}
}

type WindowScanner struct {
	Tuples []*xsql.Tuple
}

func (s *WindowScanner) addTuple(tuple *xsql.Tuple) {
	s.Tuples = append(s.Tuples, tuple)
}

// scan left-open, right-closed window
func (s *WindowScanner) scanWindow(windowStart, windowEnd time.Time) []*xsql.Tuple {
	result := make([]*xsql.Tuple, 0)
	for _, tuple := range s.Tuples {
		if tuple.Timestamp.After(windowStart) && (tuple.Timestamp.Before(windowEnd) || tuple.Timestamp.Equal(windowEnd)) {
			result = append(result, tuple)
		} else if tuple.Timestamp.After(windowEnd) {
			break
		}
	}
	return result
}

// gc the tuples which earlier than gcTime
func (s *WindowScanner) gc(gcTime time.Time) {
	if len(s.Tuples) < 1 {
		return
	}
	index := -1
	for i, tuple := range s.Tuples {
		if tuple.Timestamp.After(gcTime) {
			index = i
			break
		}
	}
	if index == -1 {
		s.Tuples = make([]*xsql.Tuple, 0)
		return
	}
	s.Tuples = s.Tuples[index:]
}
