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
	"fmt"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

type EventSlidingWindowOp struct {
	*WindowV2Operator
	Delay            time.Duration
	Length           time.Duration
	stateFuncs       []*ast.Call
	triggerCondition ast.Expr
	state            *EventSlidingWindowV2State
}

// EventSlidingWindowV2State is published once, mutated only by the operator
// goroutine, and frozen by the checkpoint barrier.
type EventSlidingWindowV2State struct {
	Scanner *WindowScanner
	DelayTS []time.Time
}

func NewEventSlidingWindowOp(o *WindowV2Operator) *EventSlidingWindowOp {
	return &EventSlidingWindowOp{
		WindowV2Operator: o,
		Delay:            o.windowConfig.Delay,
		Length:           o.windowConfig.Length,
		stateFuncs:       o.windowConfig.StateFuncs,
		triggerCondition: o.windowConfig.TriggerCondition,
		state: &EventSlidingWindowV2State{
			Scanner: o.scanner,
			DelayTS: make([]time.Time, 0),
		},
	}
}

func (s *EventSlidingWindowOp) exec(ctx api.StreamContext, errCh chan<- error) {
	if err := s.restoreState(ctx); err != nil {
		errCh <- err
		return
	}
	fv, _ := xsql.NewFunctionValuersForOp(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case input := <-s.input:
			data, processed := s.ingest(ctx, input)
			if processed {
				continue
			}
			switch tuple := data.(type) {
			case *xsql.WatermarkTuple:
				now := tuple.GetTimestamp()
				consumed := 0
				for _, delayTs := range s.state.DelayTS {
					if delayTs.Before(now) || delayTs.Equal(now) {
						windowStart := delayTs.Add(-s.Length).Add(-s.Delay)
						windowEnd := now
						s.emitWindow(ctx, windowStart, windowEnd)
						consumed++
					} else {
						break
					}
				}
				if consumed > 0 {
					s.state.DelayTS = s.state.DelayTS[consumed:]
				}
				s.scanner.gc(now.Add(-s.Length).Add(-s.Delay))
			case *xsql.Tuple:
				s.onProcessStart(ctx, input)
				windowEnd := tuple.Timestamp
				windowStart := windowEnd.Add(-s.Length)
				s.scanner.addTuple(tuple)
				sendWindow := true
				if s.triggerCondition != nil {
					sendWindow = isMatchCondition(ctx, s.triggerCondition, fv, tuple, s.stateFuncs)
				}
				if s.Delay > 0 && sendWindow {
					s.state.DelayTS = append(s.state.DelayTS, tuple.Timestamp.Add(s.Delay))
					sendWindow = false
				}
				if sendWindow {
					s.emitWindow(ctx, windowStart, windowEnd)
				}
				s.onProcessEnd(ctx)
			}
		}
	}
}

func (s *EventSlidingWindowOp) restoreState(ctx api.StreamContext) error {
	v, err := ctx.GetState(V2WindowInputsKey)
	if err != nil {
		return err
	}
	if v != nil {
		state, ok := v.(*EventSlidingWindowV2State)
		if !ok {
			return fmt.Errorf("restore window V2 event sliding state %T error, invalid type", v)
		}
		if state.Scanner == nil {
			state.Scanner = &WindowScanner{Tuples: make([]*xsql.Tuple, 0)}
		}
		s.state = state
		s.scanner = state.Scanner
	}
	return ctx.PutState(V2WindowInputsKey, s.state)
}

func (o *WindowV2Operator) ingest(ctx api.StreamContext, item any) (any, bool) {
	ctx.GetLogger().Debugf("receive %v", item)
	item, processed := o.preprocess(ctx, item)
	if processed {
		return item, processed
	}
	switch d := item.(type) {
	case error:
		if o.sendError {
			o.Broadcast(d)
		}
		return nil, true
	case xsql.EOFTuple, xsql.BatchEOFTuple:
		o.Broadcast(d)
		return nil, true
	}
	// watermark tuple should return
	return item, false
}
