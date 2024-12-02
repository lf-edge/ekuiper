// Copyright 2024 EMQ Technologies Co., Ltd.
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

	"github.com/benbjohnson/clock"
	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	topoContext "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/topo/state"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

var EnableAlignWindow bool

func init() {
	EnableAlignWindow = true
}

type WindowIncAggOperator struct {
	*defaultSinkNode
	windowConfig *WindowConfig
	Dimensions   ast.Dimensions
	aggFields    []*ast.Field
	WindowExec   windowIncAggExec
}

func NewWindowIncAggOp(name string, w *WindowConfig, dimensions ast.Dimensions, aggFields []*ast.Field, options *def.RuleOption) (*WindowIncAggOperator, error) {
	o := new(WindowIncAggOperator)
	o.defaultSinkNode = newDefaultSinkNode(name, options)
	o.windowConfig = w
	o.Dimensions = dimensions
	o.aggFields = aggFields
	switch w.Type {
	case ast.COUNT_WINDOW:
		wExec := &CountWindowIncAggOp{
			WindowIncAggOperator: o,
			windowSize:           w.CountLength,
		}
		o.WindowExec = wExec
	case ast.TUMBLING_WINDOW:
		wExec := NewTumblingWindowIncAggOp(o)
		o.WindowExec = wExec
	case ast.SLIDING_WINDOW:
		wExec := NewSlidingWindowIncAggOp(o)
		o.WindowExec = wExec
	case ast.HOPPING_WINDOW:
		o.WindowExec = NewHoppingWindowIncAggOp(o)
	}
	return o, nil
}

func (o *WindowIncAggOperator) Close() {
	o.defaultNode.Close()
}

// Exec is the entry point for the executor
// input: *xsql.Tuple from preprocessor
// output: xsql.WindowTuplesSet
func (o *WindowIncAggOperator) Exec(ctx api.StreamContext, errCh chan<- error) {
	o.prepareExec(ctx, errCh, "op")
	go func() {
		defer o.Close()
		err := infra.SafeRun(func() error {
			o.WindowExec.exec(ctx, errCh)
			return nil
		})
		if err != nil {
			infra.DrainError(ctx, err, errCh)
		}
	}()
}

type windowIncAggExec interface {
	exec(ctx api.StreamContext, errCh chan<- error)
}

type CountWindowIncAggOp struct {
	*WindowIncAggOperator
	windowSize int

	currWindow     *IncAggWindow
	currWindowSize int
}

type IncAggWindow struct {
	StartTime             time.Time
	DimensionsIncAggRange map[string]*IncAggRange
}

type IncAggRange struct {
	fctx    *topoContext.DefaultContext
	fv      *xsql.FunctionValuer
	lastRow *xsql.Tuple
	fields  map[string]interface{}
}

func (co *CountWindowIncAggOp) exec(ctx api.StreamContext, errCh chan<- error) {
	fv, _ := xsql.NewFunctionValuersForOp(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case input := <-co.input:
			now := timex.GetNow()
			data, processed := co.commonIngest(ctx, input)
			if processed {
				continue
			}
			co.onProcessStart(ctx, input)
			switch row := data.(type) {
			case *xsql.Tuple:
				if co.currWindow == nil {
					co.currWindow = newIncAggWindow(ctx, now)
				}
				name := calDimension(fv, co.Dimensions, row)
				incAggCal(ctx, name, row, co.currWindow, co.aggFields)
				co.currWindowSize++
				if co.currWindowSize >= co.windowSize {
					co.emit(ctx, errCh)
				}
			}
			co.onProcessEnd(ctx)
		}
		co.statManager.SetBufferLength(int64(len(co.input)))
	}
}

func (co *CountWindowIncAggOp) setIncAggWindow(ctx api.StreamContext) {
	co.currWindow = &IncAggWindow{
		DimensionsIncAggRange: make(map[string]*IncAggRange),
	}
}

func (co *CountWindowIncAggOp) newIncAggRange(ctx api.StreamContext) *IncAggRange {
	fstore, _ := state.CreateStore("incAggWindow", 0)
	fctx := topoContext.Background().WithMeta(ctx.GetRuleId(), ctx.GetOpId(), fstore)
	fv, _ := xsql.NewFunctionValuersForOp(fctx)
	return &IncAggRange{
		fv:     fv,
		fields: make(map[string]interface{}),
	}
}

func (co *CountWindowIncAggOp) incAggCal(ctx api.StreamContext, dimension string, row *xsql.Tuple, incAggWindow *IncAggWindow) {
	dimensionsRange, ok := incAggWindow.DimensionsIncAggRange[dimension]
	if !ok {
		dimensionsRange = co.newIncAggRange(ctx)
		incAggWindow.DimensionsIncAggRange[dimension] = dimensionsRange
	}
	ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(dimensionsRange.fv, row, &xsql.WildcardValuer{Data: row})}
	dimensionsRange.lastRow = row
	for _, aggField := range co.aggFields {
		vi := ve.Eval(aggField.Expr)
		colName := aggField.Name
		if len(aggField.AName) > 0 {
			colName = aggField.AName
		}
		dimensionsRange.fields[colName] = vi
	}
}

func (co *CountWindowIncAggOp) emit(ctx api.StreamContext, errCh chan<- error) {
	results := &xsql.WindowTuples{
		Content: make([]xsql.Row, 0),
	}
	for _, incAggRange := range co.currWindow.DimensionsIncAggRange {
		for name, value := range incAggRange.fields {
			incAggRange.lastRow.Set(name, value)
		}
		results.Content = append(results.Content, incAggRange.lastRow)
	}
	results.WindowRange = xsql.NewWindowRange(co.currWindow.StartTime.UnixMilli(), timex.GetNow().UnixMilli())
	co.currWindowSize = 0
	co.currWindow = nil
	co.Broadcast(results)
}

type TumblingWindowIncAggOp struct {
	*WindowIncAggOperator
	ticker     *clock.Ticker
	FirstTimer *clock.Timer
	Interval   time.Duration
	currWindow *IncAggWindow
}

func NewTumblingWindowIncAggOp(o *WindowIncAggOperator) *TumblingWindowIncAggOp {
	op := &TumblingWindowIncAggOp{
		WindowIncAggOperator: o,
		Interval:             o.windowConfig.Interval,
	}
	return op
}

func (to *TumblingWindowIncAggOp) exec(ctx api.StreamContext, errCh chan<- error) {
	defer func() {
		if to.ticker != nil {
			to.ticker.Stop()
		}
	}()
	now := timex.GetNow()
	if !EnableAlignWindow {
		to.ticker = timex.GetTicker(to.Interval)
	} else {
		_, to.FirstTimer = getFirstTimer(ctx, to.windowConfig.RawInterval, to.windowConfig.TimeUnit)
		to.currWindow = newIncAggWindow(ctx, now)
	}
	fv, _ := xsql.NewFunctionValuersForOp(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case input := <-to.input:
			now := timex.GetNow()
			data, processed := to.commonIngest(ctx, input)
			if processed {
				continue
			}
			switch row := data.(type) {
			case *xsql.Tuple:
				if to.currWindow == nil {
					to.currWindow = newIncAggWindow(ctx, now)
				}
				name := calDimension(fv, to.Dimensions, row)
				incAggCal(ctx, name, row, to.currWindow, to.aggFields)
			}
		default:
		}
		if to.FirstTimer != nil {
			select {
			case <-ctx.Done():
				return
			case now := <-to.FirstTimer.C:
				to.FirstTimer.Stop()
				to.FirstTimer = nil
				if to.currWindow != nil {
					to.emit(ctx, errCh, now)
				}
				to.ticker = timex.GetTicker(to.Interval)
			default:
			}
		}
		if to.ticker != nil {
			select {
			case <-ctx.Done():
				return
			case now := <-to.ticker.C:
				if to.currWindow != nil {
					to.emit(ctx, errCh, now)
				}
			default:
			}
		}
	}
}

func (to *TumblingWindowIncAggOp) emit(ctx api.StreamContext, errCh chan<- error, now time.Time) {
	results := &xsql.WindowTuples{
		Content: make([]xsql.Row, 0),
	}
	for _, incAggRange := range to.currWindow.DimensionsIncAggRange {
		for name, value := range incAggRange.fields {
			incAggRange.lastRow.Set(name, value)
		}
		results.Content = append(results.Content, incAggRange.lastRow)
	}
	results.WindowRange = xsql.NewWindowRange(to.currWindow.StartTime.UnixMilli(), now.UnixMilli())
	to.currWindow = nil
	to.Broadcast(results)
}

type SlidingWindowIncAggOp struct {
	*WindowIncAggOperator
	triggerCondition ast.Expr
	Length           time.Duration
	Delay            time.Duration
	currWindowList   []*IncAggWindow
	taskCh           chan *IncAggOpTask
}

type IncAggOpTask struct {
	window *IncAggWindow
}

func NewSlidingWindowIncAggOp(o *WindowIncAggOperator) *SlidingWindowIncAggOp {
	op := &SlidingWindowIncAggOp{
		WindowIncAggOperator: o,
		triggerCondition:     o.windowConfig.TriggerCondition,
		Length:               o.windowConfig.Length,
		Delay:                o.windowConfig.Delay,
		currWindowList:       make([]*IncAggWindow, 0),
		taskCh:               make(chan *IncAggOpTask, 1024),
	}
	return op
}

func (so *SlidingWindowIncAggOp) exec(ctx api.StreamContext, errCh chan<- error) {
	fv, _ := xsql.NewFunctionValuersForOp(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case input := <-so.input:
			now := timex.GetNow()
			data, processed := so.commonIngest(ctx, input)
			if processed {
				continue
			}
			switch row := data.(type) {
			case *xsql.Tuple:
				so.currWindowList = gcIncAggWindow(so.currWindowList, so.Length+so.Delay, now)
				so.appendIncAggWindow(ctx, errCh, fv, row, now)
				if len(so.currWindowList) > 0 && so.isMatchCondition(ctx, fv, row) {
					if so.Delay > 0 {
						t := &IncAggOpTask{}
						go func(task *IncAggOpTask) {
							after := timex.After(so.Delay)
							select {
							case <-ctx.Done():
								return
							case <-after:
								so.taskCh <- task
							}
						}(t)
					} else {
						so.emit(ctx, errCh, so.currWindowList[0], now)
					}
				}
			}
		case <-so.taskCh:
			now := timex.GetNow()
			so.currWindowList = gcIncAggWindow(so.currWindowList, so.Length+so.Delay, now)
			if len(so.currWindowList) > 0 {
				so.emit(ctx, errCh, so.currWindowList[0], now)
			}
		}
	}
}

func (so *SlidingWindowIncAggOp) appendIncAggWindow(ctx api.StreamContext, errCh chan<- error, fv *xsql.FunctionValuer, row *xsql.Tuple, now time.Time) {
	name := calDimension(fv, so.Dimensions, row)
	so.currWindowList = append(so.currWindowList, newIncAggWindow(ctx, now))
	for _, incWindow := range so.currWindowList {
		incAggCal(ctx, name, row, incWindow, so.aggFields)
	}
}

func (so *SlidingWindowIncAggOp) emit(ctx api.StreamContext, errCh chan<- error, window *IncAggWindow, now time.Time) {
	results := &xsql.WindowTuples{
		Content: make([]xsql.Row, 0),
	}
	for _, incAggRange := range window.DimensionsIncAggRange {
		for name, value := range incAggRange.fields {
			incAggRange.lastRow.Set(name, value)
		}
		results.Content = append(results.Content, incAggRange.lastRow)
	}
	results.WindowRange = xsql.NewWindowRange(window.StartTime.UnixMilli(), now.UnixMilli())
	so.Broadcast(results)
}

func (so *SlidingWindowIncAggOp) isMatchCondition(ctx api.StreamContext, fv *xsql.FunctionValuer, d *xsql.Tuple) bool {
	if so.triggerCondition == nil {
		return true
	}
	log := ctx.GetLogger()
	ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(d, fv)}
	result := ve.Eval(so.triggerCondition)
	// not match trigger condition
	if result == nil {
		return false
	}
	switch v := result.(type) {
	case error:
		log.Errorf("inc sliding window trigger condition meet error: %v", v)
		return false
	case bool:
		return v
	default:
		return false
	}
}

type HoppingWindowIncAggOp struct {
	*WindowIncAggOperator
	FirstTimer     *clock.Timer
	ticker         *clock.Ticker
	Length         time.Duration
	Interval       time.Duration
	currWindowList []*IncAggWindow
	taskCh         chan *IncAggOpTask
}

func NewHoppingWindowIncAggOp(o *WindowIncAggOperator) *HoppingWindowIncAggOp {
	op := &HoppingWindowIncAggOp{
		WindowIncAggOperator: o,
		Length:               o.windowConfig.Length,
		Interval:             o.windowConfig.Interval,
		currWindowList:       make([]*IncAggWindow, 0),
		taskCh:               make(chan *IncAggOpTask, 1024),
	}
	return op
}

func (ho *HoppingWindowIncAggOp) exec(ctx api.StreamContext, errCh chan<- error) {
	defer func() {
		if ho.ticker != nil {
			ho.ticker.Stop()
		}
	}()
	now := time.Now()
	if !EnableAlignWindow {
		ho.ticker = timex.GetTicker(ho.Interval)
		ho.newIncWindow(ctx, now)
	} else {
		_, ho.FirstTimer = getFirstTimer(ctx, ho.windowConfig.RawInterval, ho.windowConfig.TimeUnit)
		ho.currWindowList = append(ho.currWindowList, newIncAggWindow(ctx, now))
	}
	fv, _ := xsql.NewFunctionValuersForOp(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-ho.taskCh:
			now := timex.GetNow()
			ho.emit(ctx, errCh, task.window, now)
			ho.currWindowList = gcIncAggWindow(ho.currWindowList, ho.Length, now)
		case input := <-ho.input:
			now := timex.GetNow()
			data, processed := ho.commonIngest(ctx, input)
			if processed {
				continue
			}
			switch row := data.(type) {
			case *xsql.Tuple:
				ho.currWindowList = gcIncAggWindow(ho.currWindowList, ho.Length, now)
				ho.calIncAggWindow(ctx, fv, row)
			}
		default:
		}
		if ho.FirstTimer != nil {
			select {
			case <-ctx.Done():
				return
			case now := <-ho.FirstTimer.C:
				ho.FirstTimer.Stop()
				ho.FirstTimer = nil
				ho.newIncWindow(ctx, now)
				ho.currWindowList = gcIncAggWindow(ho.currWindowList, ho.Length, now)
				ho.ticker = timex.GetTicker(ho.Interval)
			default:
			}
		}
		if ho.ticker != nil {
			select {
			case <-ctx.Done():
				return
			case now := <-ho.ticker.C:
				ho.currWindowList = gcIncAggWindow(ho.currWindowList, ho.Length, now)
				ho.newIncWindow(ctx, now)
			default:
			}
		}
	}
}

func (ho *HoppingWindowIncAggOp) newIncWindow(ctx api.StreamContext, now time.Time) {
	newWindow := newIncAggWindow(ctx, now)
	ho.currWindowList = append(ho.currWindowList, newWindow)
	go func() {
		after := timex.After(ho.Length)
		select {
		case <-ctx.Done():
			return
		case <-after:
			ho.taskCh <- &IncAggOpTask{window: newWindow}
		}
	}()
}

func (ho *HoppingWindowIncAggOp) emit(ctx api.StreamContext, errCh chan<- error, window *IncAggWindow, now time.Time) {
	results := &xsql.WindowTuples{
		Content: make([]xsql.Row, 0),
	}
	for _, incAggRange := range window.DimensionsIncAggRange {
		for name, value := range incAggRange.fields {
			incAggRange.lastRow.Set(name, value)
		}
		results.Content = append(results.Content, incAggRange.lastRow)
	}
	results.WindowRange = xsql.NewWindowRange(window.StartTime.UnixMilli(), now.UnixMilli())
	ho.Broadcast(results)
}

func (ho *HoppingWindowIncAggOp) calIncAggWindow(ctx api.StreamContext, fv *xsql.FunctionValuer, row *xsql.Tuple) {
	name := calDimension(fv, ho.Dimensions, row)
	for _, incWindow := range ho.currWindowList {
		incAggCal(ctx, name, row, incWindow, ho.aggFields)
	}
}

func incAggCal(ctx api.StreamContext, dimension string, row *xsql.Tuple, incAggWindow *IncAggWindow, aggFields []*ast.Field) {
	dimensionsRange, ok := incAggWindow.DimensionsIncAggRange[dimension]
	if !ok {
		dimensionsRange = newIncAggRange(ctx)
		incAggWindow.DimensionsIncAggRange[dimension] = dimensionsRange
	}
	cloneRow := cloneTuple(row)
	ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(dimensionsRange.fv, cloneRow, &xsql.WildcardValuer{Data: cloneRow})}
	dimensionsRange.lastRow = cloneRow
	for _, aggField := range aggFields {
		vi := ve.Eval(aggField.Expr)
		colName := aggField.Name
		if len(aggField.AName) > 0 {
			colName = aggField.AName
		}
		dimensionsRange.fields[colName] = vi
	}
}

func newIncAggRange(ctx api.StreamContext) *IncAggRange {
	fstore, _ := state.CreateStore("incAggWindow", 0)
	fctx := topoContext.Background().WithMeta(ctx.GetRuleId(), ctx.GetOpId(), fstore)
	fv, _ := xsql.NewFunctionValuersForOp(fctx)
	return &IncAggRange{
		fctx:   fctx.(*topoContext.DefaultContext),
		fv:     fv,
		fields: make(map[string]interface{}),
	}
}

func newIncAggWindow(ctx api.StreamContext, now time.Time) *IncAggWindow {
	return &IncAggWindow{
		StartTime:             now,
		DimensionsIncAggRange: make(map[string]*IncAggRange),
	}
}

func calDimension(fv *xsql.FunctionValuer, dimensions ast.Dimensions, row *xsql.Tuple) string {
	name := "dim_"
	ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(row, fv, &xsql.WildcardValuer{Data: row})}
	for _, d := range dimensions {
		r := ve.Eval(d.Expr)
		if _, ok := r.(error); ok {
			continue
		} else {
			name += fmt.Sprintf("%v,", r)
		}
	}
	return name
}

func gcIncAggWindow(currWindowList []*IncAggWindow, windowLength time.Duration, now time.Time) []*IncAggWindow {
	index := 0
	for i, incAggWindow := range currWindowList {
		if now.Sub(incAggWindow.StartTime) >= windowLength {
			index = i + 1
			continue
		}
		break
	}
	if index >= len(currWindowList) {
		newList := make([]*IncAggWindow, 0)
		return newList
	}
	return currWindowList[index:]
}
