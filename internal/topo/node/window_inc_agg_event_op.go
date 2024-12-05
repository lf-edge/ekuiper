// Copyright 2024 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
)

type SlidingWindowIncAggEventOp struct {
	*SlidingWindowIncAggOp
	EmitList []*IncAggWindow
}

func NewSlidingWindowIncAggEventOp(o *WindowIncAggOperator) *SlidingWindowIncAggEventOp {
	op := &SlidingWindowIncAggEventOp{}
	op.SlidingWindowIncAggOp = NewSlidingWindowIncAggOp(o)
	op.EmitList = make([]*IncAggWindow, 0)
	return op
}

func (so *SlidingWindowIncAggEventOp) exec(ctx api.StreamContext, errCh chan<- error) {
	fv, _ := xsql.NewFunctionValuersForOp(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case input := <-so.input:
			data, processed := so.ingest(ctx, input)
			if processed {
				break
			}
			switch tuple := data.(type) {
			case *xsql.WatermarkTuple:
				now := tuple.GetTimestamp()
				so.emitList(ctx, errCh, now)
				so.CurrWindowList = gcIncAggWindow(so.CurrWindowList, so.Length, now)
			case *xsql.Tuple:
				if so.Delay > 0 {
					so.appendDelayIncAggWindowInEvent(ctx, errCh, fv, tuple)
					continue
				}
				so.appendIncAggWindowInEvent(ctx, errCh, fv, tuple)
			}
		}
	}
}

func (so *SlidingWindowIncAggEventOp) emitList(ctx api.StreamContext, errCh chan<- error, triggerTS time.Time) {
	if len(so.EmitList) > 0 {
		triggerIndex := -1
		for index, window := range so.EmitList {
			if window.EventTime.Add(so.Delay).Compare(triggerTS) <= 0 {
				triggerIndex = index
				so.emit(ctx, errCh, window, triggerTS)
			} else {
				break
			}
		}
		// emit nothing
		if triggerIndex == -1 {
			return
		}
		// emit all windows
		if triggerIndex >= len(so.EmitList)-1 {
			so.EmitList = make([]*IncAggWindow, 0)
			return
		}
		// emit part of windows
		so.EmitList = so.EmitList[triggerIndex+1:]
	}
}

func (so *SlidingWindowIncAggEventOp) appendIncAggWindowInEvent(ctx api.StreamContext, errCh chan<- error, fv *xsql.FunctionValuer, row *xsql.Tuple) {
	now := row.GetTimestamp()
	name := calDimension(fv, so.Dimensions, row)
	if so.isMatchCondition(ctx, fv, row) {
		so.CurrWindowList = append(so.CurrWindowList, newIncAggWindow(ctx, now))
	}
	for _, incWindow := range so.CurrWindowList {
		if incWindow.StartTime.Compare(now) <= 0 && incWindow.StartTime.Add(so.Length).After(now) {
			incAggCal(ctx, name, row, incWindow, so.aggFields)
		}
	}
	if so.isMatchCondition(ctx, fv, row) {
		emitWindow := so.CurrWindowList[0].Clone(ctx)
		emitWindow.StartTime = row.GetTimestamp()
		so.EmitList = append(so.EmitList, emitWindow)
	}
	return
}

func (so *SlidingWindowIncAggEventOp) appendDelayIncAggWindowInEvent(ctx api.StreamContext, errCh chan<- error, fv *xsql.FunctionValuer, row *xsql.Tuple) {
	now := row.GetTimestamp()
	name := calDimension(fv, so.Dimensions, row)
	so.CurrWindowList = append(so.CurrWindowList, newIncAggWindow(ctx, row.GetTimestamp()))
	for _, incWindow := range so.CurrWindowList {
		if incWindow.StartTime.Compare(now) <= 0 && incWindow.StartTime.Add(so.Length).After(now) {
			incAggCal(ctx, name, row, incWindow, so.aggFields)
		}
	}
	for _, incWindow := range so.EmitList {
		if incWindow.EventTime.Compare(now) <= 0 && incWindow.EventTime.Add(so.Delay).After(now) {
			incAggCal(ctx, name, row, incWindow, so.aggFields)
		}
	}
	if so.isMatchCondition(ctx, fv, row) {
		emitWindow := so.CurrWindowList[0].Clone(ctx)
		emitWindow.EventTime = row.GetTimestamp()
		so.EmitList = append(so.EmitList, emitWindow)
	}
}

func (o *WindowIncAggOperator) ingest(ctx api.StreamContext, item any) (any, bool) {
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
	case xsql.EOFTuple:
		o.Broadcast(d)
		return nil, true
	}
	// watermark tuple should return
	return item, false
}
