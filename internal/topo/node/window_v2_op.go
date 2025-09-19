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
	*StateWindowStatus
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
		StateWindowStatus: &StateWindowStatus{
			Scanner: o.scanner,
		},
	}
}

func (s *StateWindowOp) emit(ctx api.StreamContext, startTime, endTime time.Time) {
	tuples := s.scanner.scanWindow(time.Time{}, InfTime)
	results := &xsql.WindowTuples{
		Content: make([]xsql.Row, 0),
	}
	for _, tuple := range tuples {
		results.Content = append(results.Content, tuple)
	}
	results.WindowRange = xsql.NewWindowRange(startTime.UnixMilli(), endTime.UnixMilli(), endTime.UnixMilli())
	s.Broadcast(results)
	s.onSend(ctx, results)
}

func (s *StateWindowOp) exec(ctx api.StreamContext, errCh chan<- error) {
	v, err := ctx.GetState(V2WindowInputsKey)
	if err == nil && v != nil {
		preStatus, ok := v.(*StateWindowStatus)
		if ok {
			s.StateWindowStatus = preStatus
			s.WindowV2Operator.scanner = preStatus.Scanner
		}
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
				if s.BeginCondition != nil && s.EmitCondition != nil {
					s.handleTupleWithBeginEmitCondition(ctx, fv, row)
				} else if s.SingleCondition != nil {
					s.handleTupleWithSingleCondition(ctx, fv, row)
				}
			}
			ctx.PutState(V2WindowInputsKey, s.StateWindowStatus)
			s.onProcessEnd(ctx)
		}
	}
}

func (s *StateWindowOp) handleTupleWithBeginEmitCondition(ctx api.StreamContext, fv *xsql.FunctionValuer, row *xsql.Tuple) {
	if !s.OnBegin {
		canBegin := isMatchCondition(ctx, s.BeginCondition, fv, row, s.stateFuncs)
		if canBegin {
			s.StartTime = row.Timestamp
			s.OnBegin = true
			s.scanner.addTuple(row)
		}
	} else {
		s.scanner.addTuple(row)
		canEmit := isMatchCondition(ctx, s.EmitCondition, fv, row, s.stateFuncs)
		if canEmit {
			s.EndTime = row.Timestamp
			s.emit(ctx, s.StartTime, s.EndTime)
			s.scanner.gc(InfTime)
			s.OnBegin = false
		}
	}
}

func (s *StateWindowOp) handleTupleWithSingleCondition(ctx api.StreamContext, fv *xsql.FunctionValuer, row *xsql.Tuple) {
	if !s.OnBegin {
		canBegin := isMatchCondition(ctx, s.SingleCondition, fv, row, s.stateFuncs)
		if canBegin {
			s.StartTime = row.Timestamp
			s.OnBegin = true
			s.scanner.addTuple(row)
		}
	} else {
		canEmit := isMatchCondition(ctx, s.SingleCondition, fv, row, s.stateFuncs)
		if canEmit {
			s.EndTime = row.Timestamp
			s.emit(ctx, s.StartTime, s.EndTime)
			s.scanner.gc(InfTime)
			s.OnBegin = true
			s.scanner.addTuple(row)
			s.StartTime = row.Timestamp
		} else {
			s.scanner.addTuple(row)
		}
	}
}

type SlidingWindowOp struct {
	*WindowV2Operator
	Delay            time.Duration
	Length           time.Duration
	stateFuncs       []*ast.Call
	triggerCondition ast.Expr
	delayNotify      chan time.Time
}

func NewSlidingWindowOp(o *WindowV2Operator) *SlidingWindowOp {
	return &SlidingWindowOp{
		WindowV2Operator: o,
		Delay:            o.windowConfig.Delay,
		Length:           o.windowConfig.Length,
		stateFuncs:       o.windowConfig.StateFuncs,
		triggerCondition: o.windowConfig.TriggerCondition,
		delayNotify:      make(chan time.Time, 1024),
	}
}

func (s *SlidingWindowOp) exec(ctx api.StreamContext, errCh chan<- error) {
	fv, _ := xsql.NewFunctionValuersForOp(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case delayTs := <-s.delayNotify:
			windowEnd := delayTs
			windowStart := delayTs.Add(-s.Delay).Add(-s.Length)
			s.emitWindow(ctx, windowStart, windowEnd)
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
				s.scanner.gc(windowStart)
				s.scanner.addTuple(row)
				sendWindow := true
				if s.triggerCondition != nil {
					sendWindow = isMatchCondition(ctx, s.triggerCondition, fv, row, s.stateFuncs)
				}
				if s.Delay > 0 && sendWindow {
					sendWindow = false
					go func(ts time.Time) {
						after := timex.After(s.Delay)
						select {
						case <-ctx.Done():
							return
						case <-after:
							s.delayNotify <- ts
						}
					}(windowEnd.Add(s.Delay))
				}
				if sendWindow {
					s.emitWindow(ctx, windowStart, windowEnd)
				}
			}
			s.onProcessEnd(ctx)
		}
	}
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
