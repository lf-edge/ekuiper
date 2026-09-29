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
	delayTS          []time.Time
}

func NewEventSlidingWindowOp(o *WindowV2Operator) *EventSlidingWindowOp {
	return &EventSlidingWindowOp{
		WindowV2Operator: o,
		Delay:            o.windowConfig.Delay,
		Length:           o.windowConfig.Length,
		stateFuncs:       o.windowConfig.StateFuncs,
		triggerCondition: o.windowConfig.TriggerCondition,
		delayTS:          make([]time.Time, 0),
	}
}

func (s *EventSlidingWindowOp) exec(ctx api.StreamContext, errCh chan<- error) {
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
				newIndex := -1
				for i, delayTs := range s.delayTS {
					if delayTs.Before(now) || delayTs.Equal(now) {
						windowStart := delayTs.Add(-s.Length).Add(-s.Delay)
						windowEnd := now
						s.emitWindow(ctx, windowStart, windowEnd)
					} else {
						newIndex = i
						break
					}
				}
				if newIndex != -1 {
					s.delayTS = s.delayTS[newIndex:]
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
					s.delayTS = append(s.delayTS, tuple.Timestamp.Add(s.Delay))
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
