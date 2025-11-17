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
	"fmt"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/xsql"
)

type HoppingWindowIncAggEventOp struct {
	op *HoppingWindowIncAggOp
	HoppingWindowIncAggEventOpState
}

type HoppingWindowIncAggEventOpState struct {
	CurrWindowList        []*IncAggWindow
	NextTriggerWindowTime time.Time
}

func NewHoppingWindowIncAggEventOp(o *WindowIncAggOperator) *HoppingWindowIncAggEventOp {
	op := &HoppingWindowIncAggEventOp{}
	op.op = NewHoppingWindowIncAggOp(o)
	op.HoppingWindowIncAggEventOpState.CurrWindowList = make([]*IncAggWindow, 0)
	return op
}

func (ho *HoppingWindowIncAggEventOp) PutState(ctx api.StreamContext) {
	for index, window := range ho.CurrWindowList {
		window.GenerateAllFunctionState()
		ho.CurrWindowList[index] = window
	}
	ctx.PutState(buildStateKey(ctx), ho.HoppingWindowIncAggEventOpState)
}

func (ho *HoppingWindowIncAggEventOp) RestoreFromState(ctx api.StreamContext) error {
	s, err := ctx.GetState(buildStateKey(ctx))
	if err != nil {
		return err
	}
	if s == nil {
		return nil
	}
	coState, ok := s.(HoppingWindowIncAggEventOpState)
	if !ok {
		return fmt.Errorf("not HoppingWindowIncAggEventOpState")
	}
	ho.HoppingWindowIncAggEventOpState = coState
	for index, window := range ho.CurrWindowList {
		window.restoreState(ctx)
		ho.CurrWindowList[index] = window
	}
	return nil
}

func (ho *HoppingWindowIncAggEventOp) exec(ctx api.StreamContext, errCh chan<- error) {
	if err := ho.RestoreFromState(ctx); err != nil {
		errCh <- err
		return
	}
	fv, _ := xsql.NewFunctionValuersForOp(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case input := <-ho.op.input:
			data, processed := ho.op.ingest(ctx, input)
			if processed {
				break
			}
			switch tuple := data.(type) {
			case *xsql.WatermarkTuple:
				now := tuple.GetTimestamp()
				ho.emitWindow(ctx, errCh, now)
				ho.CurrWindowList = gcIncAggWindow(ho.CurrWindowList, ho.op.Length, now)
				ho.PutState(ctx)
			case *xsql.Tuple:
				ho.op.onProcessStart(ctx, data)
				now := tuple.GetTimestamp()
				ho.triggerWindow(ctx, now)
				ho.calIncAggWindow(ctx, fv, tuple, tuple.GetTimestamp())
				ho.PutState(ctx)
				ho.op.onProcessEnd(ctx)
			}
		}
	}
}

func (ho *HoppingWindowIncAggEventOp) calIncAggWindow(ctx api.StreamContext, fv *xsql.FunctionValuer, row *xsql.Tuple, now time.Time) {
	name := calDimension(fv, ho.op.Dimensions, row)
	for _, incWindow := range ho.CurrWindowList {
		if incWindow.StartTime.Compare(now) <= 0 && incWindow.StartTime.Add(ho.op.Length).After(now) {
			incAggCal(ctx, name, row, incWindow, ho.op.aggFields)
		}
	}
}

func (ho *HoppingWindowIncAggEventOp) emitWindow(ctx api.StreamContext, errCh chan<- error, now time.Time) {
	for _, incWindow := range ho.CurrWindowList {
		if incWindow.StartTime.Add(ho.op.Length).Compare(now) <= 0 {
			ho.op.emit(ctx, errCh, incWindow, incWindow.StartTime.Add(ho.op.Length))
		}
	}
}

func (ho *HoppingWindowIncAggEventOp) calIncAggWindowInEvent(ctx api.StreamContext, fv *xsql.FunctionValuer, row *xsql.Tuple) {
	name := calDimension(fv, ho.op.Dimensions, row)
	for _, incWindow := range ho.CurrWindowList {
		if incWindow.StartTime.Compare(row.GetTimestamp()) <= 0 && incWindow.StartTime.Add(ho.op.Length).After(row.GetTimestamp()) {
			incAggCal(ctx, name, row, incWindow, ho.op.aggFields)
		}
	}
}

func (ho *HoppingWindowIncAggEventOp) triggerWindow(ctx api.StreamContext, now time.Time) {
	next := getAlignedWindowEndTime(now, ho.op.windowConfig.RawInterval, ho.op.windowConfig.TimeUnit)
	if ho.NextTriggerWindowTime.Before(now) {
		ho.NextTriggerWindowTime = next
		ho.CurrWindowList = append(ho.CurrWindowList, newIncAggWindow(ctx, next.Add(-ho.op.Interval)))
	}
}

type SlidingWindowIncAggEventOp struct {
	op *SlidingWindowIncAggOp
	SlidingWindowIncAggEventOpState
}

type SlidingWindowIncAggEventOpState struct {
	SlidingWindowIncAggOpState
	EmitList []*IncAggWindow
}

func NewSlidingWindowIncAggEventOp(o *WindowIncAggOperator) *SlidingWindowIncAggEventOp {
	op := &SlidingWindowIncAggEventOp{}
	op.op = NewSlidingWindowIncAggOp(o)
	op.CurrWindowList = make([]*IncAggWindow, 0)
	op.EmitList = make([]*IncAggWindow, 0)
	return op
}

func (so *SlidingWindowIncAggEventOp) PutState(ctx api.StreamContext) {
	for index, window := range so.CurrWindowList {
		window.GenerateAllFunctionState()
		so.CurrWindowList[index] = window
	}
	for index, window := range so.EmitList {
		window.GenerateAllFunctionState()
		so.EmitList[index] = window
	}
	ctx.PutState(buildStateKey(ctx), so.SlidingWindowIncAggEventOpState)
}

func (so *SlidingWindowIncAggEventOp) RestoreFromState(ctx api.StreamContext) error {
	s, err := ctx.GetState(buildStateKey(ctx))
	if err != nil {
		return err
	}
	if s == nil {
		return nil
	}
	soState, ok := s.(SlidingWindowIncAggEventOpState)
	if !ok {
		return fmt.Errorf("not SlidingWindowIncAggEventOpState")
	}
	so.SlidingWindowIncAggEventOpState = soState
	for index, window := range so.CurrWindowList {
		window.GenerateAllFunctionState()
		so.CurrWindowList[index] = window
	}
	for index, window := range so.EmitList {
		window.GenerateAllFunctionState()
		so.EmitList[index] = window
	}
	return nil
}

func (so *SlidingWindowIncAggEventOp) exec(ctx api.StreamContext, errCh chan<- error) {
	if err := so.RestoreFromState(ctx); err != nil {
		errCh <- err
		return
	}
	fv, _ := xsql.NewFunctionValuersForOp(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case input := <-so.op.input:
			data, processed := so.op.ingest(ctx, input)
			if processed {
				break
			}
			switch tuple := data.(type) {
			case *xsql.WatermarkTuple:
				now := tuple.GetTimestamp()
				so.emitList(ctx, errCh, now)
				so.CurrWindowList = gcIncAggWindow(so.CurrWindowList, so.op.Length, now)
				so.PutState(ctx)
			case *xsql.Tuple:
				so.op.onProcessStart(ctx, tuple)
				if so.op.Delay > 0 {
					so.appendDelayIncAggWindowInEvent(ctx, errCh, fv, tuple)
					so.PutState(ctx)
					so.op.onProcessEnd(ctx)
					continue
				}
				so.appendIncAggWindowInEvent(ctx, errCh, fv, tuple)
				so.PutState(ctx)
				so.op.onProcessEnd(ctx)
			}
		}
	}
}

func (so *SlidingWindowIncAggEventOp) emitList(ctx api.StreamContext, errCh chan<- error, triggerTS time.Time) {
	if len(so.EmitList) > 0 {
		triggerIndex := -1
		for index, window := range so.EmitList {
			if window.EventTime.Add(so.op.Delay).Compare(triggerTS) <= 0 {
				triggerIndex = index
				so.op.emit(ctx, errCh, window, triggerTS)
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
	name := calDimension(fv, so.op.Dimensions, row)
	if so.op.isMatchCondition(ctx, fv, row) {
		so.CurrWindowList = append(so.CurrWindowList, newIncAggWindow(ctx, now))
	}
	for _, incWindow := range so.CurrWindowList {
		if incWindow.StartTime.Compare(now) <= 0 && incWindow.StartTime.Add(so.op.Length).After(now) {
			incAggCal(ctx, name, row, incWindow, so.op.aggFields)
		}
	}
	if so.op.isMatchCondition(ctx, fv, row) {
		emitWindow := so.CurrWindowList[0].Clone(ctx)
		emitWindow.StartTime = row.GetTimestamp()
		so.EmitList = append(so.EmitList, emitWindow)
	}
}

func (so *SlidingWindowIncAggEventOp) appendDelayIncAggWindowInEvent(ctx api.StreamContext, errCh chan<- error, fv *xsql.FunctionValuer, row *xsql.Tuple) {
	now := row.GetTimestamp()
	name := calDimension(fv, so.op.Dimensions, row)
	so.CurrWindowList = append(so.CurrWindowList, newIncAggWindow(ctx, row.GetTimestamp()))
	for _, incWindow := range so.CurrWindowList {
		if incWindow.StartTime.Compare(now) <= 0 && incWindow.StartTime.Add(so.op.Length).After(now) {
			incAggCal(ctx, name, row, incWindow, so.op.aggFields)
		}
	}
	for _, incWindow := range so.EmitList {
		if incWindow.EventTime.Compare(now) <= 0 && incWindow.EventTime.Add(so.op.Delay).After(now) {
			incAggCal(ctx, name, row, incWindow, so.op.aggFields)
		}
	}
	if so.op.isMatchCondition(ctx, fv, row) {
		emitWindow := so.CurrWindowList[0].Clone(ctx)
		emitWindow.EventTime = row.GetTimestamp()
		so.EmitList = append(so.EmitList, emitWindow)
	}
}

type TumblingWindowIncAggEventOp struct {
	*HoppingWindowIncAggEventOp
}

func NewTumblingWindowIncAggEventOp(o *WindowIncAggOperator) *TumblingWindowIncAggEventOp {
	op := &TumblingWindowIncAggEventOp{}
	op.HoppingWindowIncAggEventOp = NewHoppingWindowIncAggEventOp(o)
	op.op.Length = o.windowConfig.Interval
	return op
}

func (to *TumblingWindowIncAggEventOp) exec(ctx api.StreamContext, errCh chan<- error) {
	to.HoppingWindowIncAggEventOp.exec(ctx, errCh)
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

type CountWindowIncAggEventOp struct {
	op *CountWindowIncAggOp
	CountWindowIncAggEventOpState
}

type CountWindowIncAggEventOpState struct {
	CurrWindow     *IncAggWindow
	CurrWindowSize int
	EmitList       []*IncAggWindow
}

func NewCountWindowIncAggEventOp(o *WindowIncAggOperator) *CountWindowIncAggEventOp {
	op := &CountWindowIncAggEventOp{
		op: &CountWindowIncAggOp{
			WindowIncAggOperator: o,
			windowSize:           o.windowConfig.CountLength,
		},
	}
	op.EmitList = make([]*IncAggWindow, 0)
	return op
}

func (co *CountWindowIncAggEventOp) exec(ctx api.StreamContext, errCh chan<- error) {
	if err := co.RestoreFromState(ctx); err != nil {
		errCh <- err
		return
	}
	fv, _ := xsql.NewFunctionValuersForOp(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case input := <-co.op.input:
			data, processed := co.op.ingest(ctx, input)
			if processed {
				break
			}
			switch tuple := data.(type) {
			case *xsql.WatermarkTuple:
				now := tuple.GetTimestamp()
				var index int
				for i, window := range co.EmitList {
					if window.StartTime.Compare(now) <= 0 {
						co.emitWindow(ctx, errCh, window, now)
						index = i
					} else {
						break
					}
				}
				if index == len(co.EmitList)-1 {
					co.EmitList = make([]*IncAggWindow, 0)
				} else {
					co.EmitList = co.EmitList[index+1:]
				}
				co.PutState(ctx)
			case *xsql.Tuple:
				co.op.onProcessStart(ctx, tuple)
				now := tuple.GetTimestamp()
				if co.CurrWindow == nil {
					co.CurrWindow = newIncAggWindow(ctx, now)
				}
				name := calDimension(fv, co.op.Dimensions, tuple)
				incAggCal(ctx, name, tuple, co.CurrWindow, co.op.aggFields)
				co.CurrWindowSize++
				if co.CurrWindowSize >= co.op.windowSize {
					co.EmitList = append(co.EmitList, co.CurrWindow)
					co.CurrWindow = nil
				}
				co.PutState(ctx)
				co.op.onProcessEnd(ctx)
			}
		}
	}
}

func (co *CountWindowIncAggEventOp) emitWindow(ctx api.StreamContext, errCh chan<- error, window *IncAggWindow, now time.Time) {
	results := &xsql.WindowTuples{
		Content: make([]xsql.Row, 0),
	}
	for _, incAggRange := range window.DimensionsIncAggRange {
		for name, value := range incAggRange.Fields {
			incAggRange.LastRow.Set(name, value)
		}
		results.Content = append(results.Content, incAggRange.LastRow)
	}
	results.WindowRange = xsql.NewWindowRange(window.StartTime.UnixMilli(), now.UnixMilli(), now.UnixMilli())
	co.op.Broadcast(results)
	co.op.onSend(ctx, results)
}

func (co *CountWindowIncAggEventOp) PutState(ctx api.StreamContext) {
	co.CurrWindow.GenerateAllFunctionState()
	ctx.PutState(buildStateKey(ctx), co.CountWindowIncAggEventOpState)
}

func (co *CountWindowIncAggEventOp) RestoreFromState(ctx api.StreamContext) error {
	s, err := ctx.GetState(buildStateKey(ctx))
	if err != nil {
		return err
	}
	if s == nil {
		return nil
	}
	coState, ok := s.(CountWindowIncAggEventOpState)
	if !ok {
		return fmt.Errorf("not CountWindowIncAggEventOpState")
	}
	co.CountWindowIncAggEventOpState = coState
	co.CurrWindow.restoreState(ctx)
	return nil
}
