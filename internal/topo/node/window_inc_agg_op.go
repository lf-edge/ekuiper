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

package node

import (
	"context"
	"encoding/gob"
	"fmt"
	"sync"
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
	gob.Register(map[string]interface{}{})
	gob.Register(&xsql.Tuple{})
	gob.Register(&IncAggRange{})
	gob.Register(map[string]*IncAggWindow{})
	gob.Register(time.Time{})
	gob.Register(&IncAggWindow{})
	gob.Register([]*IncAggWindow{})
	gob.Register(CountWindowIncAggOpState{})
	gob.Register(TumblingWindowIncAggOpState{})
	gob.Register(SlidingWindowIncAggOpState{})
	gob.Register(SlidingWindowIncAggEventOpState{})
}

type WindowIncAggOperator struct {
	*defaultSinkNode
	windowConfig *WindowConfig
	Dimensions   ast.Dimensions
	aggFields    []*ast.Field
	WindowExec   windowIncAggExec

	putStateReqCh chan chan error
	restoreReqCh  chan chan error

	firstTimerMu      sync.Mutex
	firstTimerCreated bool
}

func NewWindowIncAggOp(name string, w *WindowConfig, dimensions ast.Dimensions, aggFields []*ast.Field, options *def.RuleOption) (*WindowIncAggOperator, error) {
	o := new(WindowIncAggOperator)
	o.defaultSinkNode = newDefaultSinkNode(name, options)
	o.windowConfig = w
	o.Dimensions = dimensions
	o.aggFields = aggFields
	o.putStateReqCh = make(chan chan error, 2)
	o.restoreReqCh = make(chan chan error, 2)
	switch w.Type {
	case ast.COUNT_WINDOW:
		if options.IsEventTime {
			wExec := NewCountWindowIncAggEventOp(o)
			o.WindowExec = wExec
		} else {
			wExec := &CountWindowIncAggOp{
				WindowIncAggOperator: o,
				windowSize:           w.CountLength,
			}
			o.WindowExec = wExec
		}
	case ast.TUMBLING_WINDOW:
		if options.IsEventTime {
			wExec := NewTumblingWindowIncAggEventOp(o)
			o.WindowExec = wExec
		} else {
			wExec := NewTumblingWindowIncAggOp(o)
			o.WindowExec = wExec
		}
	case ast.SLIDING_WINDOW:
		if options.IsEventTime {
			wExec := NewSlidingWindowIncAggEventOp(o)
			o.WindowExec = wExec
		} else {
			wExec := NewSlidingWindowIncAggOp(o)
			o.WindowExec = wExec
		}
	case ast.HOPPING_WINDOW:
		if options.IsEventTime {
			o.WindowExec = NewHoppingWindowIncAggEventOp(o)
		} else {
			o.WindowExec = NewHoppingWindowIncAggOp(o)
		}
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

func (o *WindowIncAggOperator) PutState4Test(ctx context.Context) error {
	return o.execStateCall4Test(ctx, o.putStateReqCh)
}

func (o *WindowIncAggOperator) RestoreFromState4Test(ctx context.Context) error {
	return o.execStateCall4Test(ctx, o.restoreReqCh)
}

func (o *WindowIncAggOperator) FirstTimerCreated4Test() bool {
	o.firstTimerMu.Lock()
	defer o.firstTimerMu.Unlock()
	return o.firstTimerCreated
}

func (o *WindowIncAggOperator) markFirstTimerCreated() {
	o.firstTimerMu.Lock()
	o.firstTimerCreated = true
	o.firstTimerMu.Unlock()
}

func (o *WindowIncAggOperator) execStateCall4Test(ctx context.Context, reqCh chan chan error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	done := make(chan error, 1)
	const timeout = 5 * time.Second
	select {
	case reqCh <- done:
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(timeout):
		return context.DeadlineExceeded
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(timeout):
		return context.DeadlineExceeded
	}
}

type windowIncAggExec interface {
	exec(ctx api.StreamContext, errCh chan<- error)
	PutState(ctx api.StreamContext)
	RestoreFromState(ctx api.StreamContext) error
}

type IncAggWindow struct {
	StartTime             time.Time
	EventTime             time.Time
	DimensionsIncAggRange map[string]*IncAggRange
}

func (w *IncAggWindow) Clone(ctx api.StreamContext) *IncAggWindow {
	c := &IncAggWindow{StartTime: w.StartTime, DimensionsIncAggRange: map[string]*IncAggRange{}}
	for k, v := range w.DimensionsIncAggRange {
		c.DimensionsIncAggRange[k] = v.Clone(ctx)
	}
	return c
}

func (w *IncAggWindow) GenerateAllFunctionState() {
	if w == nil {
		return
	}
	for _, r := range w.DimensionsIncAggRange {
		r.generateFunctionState()
	}
}

func (w *IncAggWindow) restoreState(ctx api.StreamContext) {
	if w == nil {
		return
	}
	for _, r := range w.DimensionsIncAggRange {
		r.restoreState(ctx)
	}
}

type IncAggRange struct {
	fv   *xsql.FunctionValuer
	fctx *topoContext.DefaultContext

	FunctionState map[string]interface{}
	LastRow       *xsql.Tuple
	Fields        map[string]interface{}
}

func (r *IncAggRange) Clone(ctx api.StreamContext) *IncAggRange {
	fstore, _ := state.CreateStore("incAggWindow", 0)
	fctx := topoContext.Background().WithMeta(ctx.GetRuleId(), ctx.GetOpId(), fstore)
	for k, v := range r.generateFunctionState() {
		fctx.PutState(k, v)
	}
	fv, _ := xsql.NewFunctionValuersForOp(fctx)
	c := &IncAggRange{
		fctx:    fctx.(*topoContext.DefaultContext),
		fv:      fv,
		LastRow: r.LastRow.Clone().(*xsql.Tuple),
		Fields:  make(map[string]interface{}),
	}
	for k, v := range r.Fields {
		c.Fields[k] = v
	}
	return c
}

func (r *IncAggRange) generateFunctionState() map[string]interface{} {
	r.FunctionState = r.fctx.GetAllState()
	return r.FunctionState
}

func (r *IncAggRange) restoreState(ctx api.StreamContext) {
	fstore, _ := state.CreateStore("incAggWindow", 0)
	fctx := topoContext.Background().WithMeta(ctx.GetRuleId(), ctx.GetOpId(), fstore)
	for k, v := range r.FunctionState {
		fctx.PutState(k, v)
	}
	fv, _ := xsql.NewFunctionValuersForOp(fctx)
	r.fctx = fctx.(*topoContext.DefaultContext)
	r.fv = fv
}

type CountWindowIncAggOp struct {
	*WindowIncAggOperator
	windowSize int
	CountWindowIncAggOpState
}

type CountWindowIncAggOpState struct {
	CurrWindow     *IncAggWindow
	CurrWindowSize int
}

func (co *CountWindowIncAggOp) PutState(ctx api.StreamContext) {
	co.CountWindowIncAggOpState.CurrWindow.GenerateAllFunctionState()
	ctx.PutState(buildStateKey(ctx), co.CountWindowIncAggOpState)
}

func (co *CountWindowIncAggOp) RestoreFromState(ctx api.StreamContext) error {
	s, err := ctx.GetState(buildStateKey(ctx))
	if err != nil {
		return err
	}
	if s == nil {
		return nil
	}
	coState, ok := s.(CountWindowIncAggOpState)
	if !ok {
		return fmt.Errorf("not CountWindowIncAggOpState")
	}
	co.CountWindowIncAggOpState = coState
	co.CountWindowIncAggOpState.CurrWindow.restoreState(ctx)
	return nil
}

func (co *CountWindowIncAggOp) exec(ctx api.StreamContext, errCh chan<- error) {
	if err := co.RestoreFromState(ctx); err != nil {
		errCh <- err
		return
	}
	fv, _ := xsql.NewFunctionValuersForOp(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case done := <-co.putStateReqCh:
			co.PutState(ctx)
			done <- nil
		case done := <-co.restoreReqCh:
			done <- co.RestoreFromState(ctx)
		case input := <-co.input:
			now := timex.GetNow()
			data, processed := co.commonIngest(ctx, input)
			if processed {
				continue
			}
			co.onProcessStart(ctx, input)
			switch row := data.(type) {
			case *xsql.Tuple:
				if co.CurrWindow == nil {
					co.CurrWindow = newIncAggWindow(ctx, now)
				}
				name := calDimension(fv, co.Dimensions, row)
				incAggCal(ctx, name, row, co.CurrWindow, co.aggFields)
				co.CurrWindowSize++
				if co.CurrWindowSize >= co.windowSize {
					co.emit(ctx, errCh)
				}
			}
			co.PutState(ctx)
			co.onProcessEnd(ctx)
		}
		co.statManager.SetBufferLength(int64(len(co.input)))
	}
}

func (co *CountWindowIncAggOp) setIncAggWindow(ctx api.StreamContext) {
	co.CurrWindow = &IncAggWindow{
		DimensionsIncAggRange: make(map[string]*IncAggRange),
	}
}

func (co *CountWindowIncAggOp) incAggCal(ctx api.StreamContext, dimension string, row *xsql.Tuple, incAggWindow *IncAggWindow) {
	dimensionsRange, ok := incAggWindow.DimensionsIncAggRange[dimension]
	if !ok {
		dimensionsRange = newIncAggRange(ctx)
		incAggWindow.DimensionsIncAggRange[dimension] = dimensionsRange
	}
	ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(dimensionsRange.fv, row, &xsql.WildcardValuer{Data: row})}
	dimensionsRange.LastRow = row
	for _, aggField := range co.aggFields {
		vi := ve.Eval(aggField.Expr)
		colName := aggField.Name
		if len(aggField.AName) > 0 {
			colName = aggField.AName
		}
		dimensionsRange.Fields[colName] = vi
	}
}

func (co *CountWindowIncAggOp) emit(ctx api.StreamContext, errCh chan<- error) {
	results := &xsql.WindowTuples{
		Content: make([]xsql.Row, 0),
	}
	for _, incAggRange := range co.CurrWindow.DimensionsIncAggRange {
		for name, value := range incAggRange.Fields {
			incAggRange.LastRow.Set(name, value)
		}
		results.Content = append(results.Content, incAggRange.LastRow)
	}
	results.WindowRange = xsql.NewWindowRange(co.CurrWindow.StartTime.UnixMilli(), timex.GetNow().UnixMilli(), timex.GetNow().UnixMilli())
	co.CurrWindowSize = 0
	co.CurrWindow = nil
	co.Broadcast(results)
	co.onSend(ctx, results)
}

type TumblingWindowIncAggOp struct {
	*WindowIncAggOperator
	ticker     *clock.Ticker
	FirstTimer *clock.Timer
	Interval   time.Duration
	TumblingWindowIncAggOpState
}

type TumblingWindowIncAggOpState struct {
	CurrWindow *IncAggWindow
}

func NewTumblingWindowIncAggOp(o *WindowIncAggOperator) *TumblingWindowIncAggOp {
	op := &TumblingWindowIncAggOp{
		WindowIncAggOperator: o,
		Interval:             o.windowConfig.Interval,
	}
	return op
}

func (to *TumblingWindowIncAggOp) PutState(ctx api.StreamContext) {
	to.CurrWindow.GenerateAllFunctionState()
	ctx.PutState(buildStateKey(ctx), to.TumblingWindowIncAggOpState)
}

func (to *TumblingWindowIncAggOp) RestoreFromState(ctx api.StreamContext) error {
	s, err := ctx.GetState(buildStateKey(ctx))
	if err != nil {
		return err
	}
	if s == nil {
		return nil
	}
	toState, ok := s.(TumblingWindowIncAggOpState)
	if !ok {
		return fmt.Errorf("not TumblingWindowIncAggOpState")
	}
	to.TumblingWindowIncAggOpState = toState
	to.TumblingWindowIncAggOpState.CurrWindow.restoreState(ctx)
	return nil
}

func (to *TumblingWindowIncAggOp) exec(ctx api.StreamContext, errCh chan<- error) {
	if err := to.RestoreFromState(ctx); err != nil {
		errCh <- err
		return
	}
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
		if to.FirstTimer != nil {
			to.markFirstTimerCreated()
		}
		if to.CurrWindow == nil {
			to.CurrWindow = newIncAggWindow(ctx, now)
		}
	}
	fv, _ := xsql.NewFunctionValuersForOp(ctx)
	if to.FirstTimer != nil {
		for {
			select {
			case <-ctx.Done():
				return
			case done := <-to.putStateReqCh:
				to.PutState(ctx)
				done <- nil
			case done := <-to.restoreReqCh:
				done <- to.RestoreFromState(ctx)
			case now := <-to.FirstTimer.C:
				to.FirstTimer.Stop()
				to.FirstTimer = nil
				if to.CurrWindow != nil {
					to.emit(ctx, errCh, now)
				}
				to.ticker = timex.GetTicker(to.Interval)
				to.PutState(ctx)
				goto outer
			case input := <-to.input:
				now := timex.GetNow()
				data, processed := to.commonIngest(ctx, input)
				if processed {
					continue
				}
				to.onProcessStart(ctx, input)
				switch row := data.(type) {
				case *xsql.Tuple:
					if to.CurrWindow == nil {
						to.CurrWindow = newIncAggWindow(ctx, now)
					}
					name := calDimension(fv, to.Dimensions, row)
					incAggCal(ctx, name, row, to.CurrWindow, to.aggFields)
				}
				to.PutState(ctx)
				to.onProcessEnd(ctx)
			}
		}
	}
outer:
	for {
		select {
		case <-ctx.Done():
			return
		case done := <-to.putStateReqCh:
			to.PutState(ctx)
			done <- nil
		case done := <-to.restoreReqCh:
			done <- to.RestoreFromState(ctx)
		case input := <-to.input:
			now := timex.GetNow()
			data, processed := to.commonIngest(ctx, input)
			if processed {
				continue
			}
			to.onProcessStart(ctx, input)
			switch row := data.(type) {
			case *xsql.Tuple:
				if to.CurrWindow == nil {
					to.CurrWindow = newIncAggWindow(ctx, now)
				}
				name := calDimension(fv, to.Dimensions, row)
				incAggCal(ctx, name, row, to.CurrWindow, to.aggFields)
			}
			to.PutState(ctx)
			to.onProcessEnd(ctx)
		case now := <-to.ticker.C:
			if to.CurrWindow != nil {
				to.emit(ctx, errCh, now)
			}
			to.PutState(ctx)
		}
	}
}

func (to *TumblingWindowIncAggOp) emit(ctx api.StreamContext, errCh chan<- error, now time.Time) {
	results := &xsql.WindowTuples{
		Content: make([]xsql.Row, 0),
	}
	for _, incAggRange := range to.CurrWindow.DimensionsIncAggRange {
		for name, value := range incAggRange.Fields {
			incAggRange.LastRow.Set(name, value)
		}
		results.Content = append(results.Content, incAggRange.LastRow)
	}
	results.WindowRange = xsql.NewWindowRange(to.CurrWindow.StartTime.UnixMilli(), now.UnixMilli(), now.UnixMilli())
	to.CurrWindow = nil
	to.Broadcast(results)
	to.onSend(ctx, results)
}

type SlidingWindowIncAggOp struct {
	*WindowIncAggOperator
	triggerCondition ast.Expr
	Length           time.Duration
	Delay            time.Duration
	taskCh           chan *IncAggOpTask
	SlidingWindowIncAggOpState
}

type SlidingWindowIncAggOpState struct {
	CurrWindowList []*IncAggWindow
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
		taskCh:               make(chan *IncAggOpTask, 1024),
	}
	op.SlidingWindowIncAggOpState.CurrWindowList = make([]*IncAggWindow, 0)
	return op
}

func (so *SlidingWindowIncAggOp) PutState(ctx api.StreamContext) {
	for index, window := range so.CurrWindowList {
		window.GenerateAllFunctionState()
		so.CurrWindowList[index] = window
	}
	ctx.PutState(buildStateKey(ctx), so.SlidingWindowIncAggOpState)
}

func (so *SlidingWindowIncAggOp) RestoreFromState(ctx api.StreamContext) error {
	s, err := ctx.GetState(buildStateKey(ctx))
	if err != nil {
		return err
	}
	if s == nil {
		return nil
	}
	soState, ok := s.(SlidingWindowIncAggOpState)
	if !ok {
		return fmt.Errorf("not SlidingWindowIncAggOpState")
	}
	so.SlidingWindowIncAggOpState = soState
	for index, window := range so.CurrWindowList {
		window.GenerateAllFunctionState()
		so.CurrWindowList[index] = window
	}
	now := timex.GetNow()
	so.CurrWindowList = gcIncAggWindow(so.CurrWindowList, so.Length, now)
	return nil
}

func (so *SlidingWindowIncAggOp) exec(ctx api.StreamContext, errCh chan<- error) {
	if err := so.RestoreFromState(ctx); err != nil {
		errCh <- err
		return
	}
	fv, _ := xsql.NewFunctionValuersForOp(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case done := <-so.putStateReqCh:
			so.PutState(ctx)
			done <- nil
		case done := <-so.restoreReqCh:
			done <- so.RestoreFromState(ctx)
		case input := <-so.input:
			now := timex.GetNow()
			data, processed := so.commonIngest(ctx, input)
			if processed {
				continue
			}
			so.onProcessStart(ctx, input)
			switch row := data.(type) {
			case *xsql.Tuple:
				so.CurrWindowList = gcIncAggWindow(so.CurrWindowList, so.Length+so.Delay, now)
				so.appendIncAggWindow(ctx, errCh, fv, row, now)
				if so.isMatchCondition(ctx, fv, row) {
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
						so.emit(ctx, errCh, so.CurrWindowList[0], now)
					}
				}
				so.PutState(ctx)
			}
			so.onProcessEnd(ctx)
		case <-so.taskCh:
			now := timex.GetNow()
			so.CurrWindowList = gcIncAggWindow(so.CurrWindowList, so.Length+so.Delay, now)
			if len(so.CurrWindowList) > 0 {
				so.emit(ctx, errCh, so.CurrWindowList[0], now)
			}
			so.PutState(ctx)
		}
	}
}

func (so *SlidingWindowIncAggOp) appendIncAggWindow(ctx api.StreamContext, errCh chan<- error, fv *xsql.FunctionValuer, row *xsql.Tuple, now time.Time) {
	name := calDimension(fv, so.Dimensions, row)
	so.CurrWindowList = append(so.CurrWindowList, newIncAggWindow(ctx, now))
	for _, incWindow := range so.CurrWindowList {
		if incWindow.StartTime.Compare(now) <= 0 && incWindow.StartTime.Add(so.Length+so.Delay).After(now) {
			incAggCal(ctx, name, row, incWindow, so.aggFields)
		}
	}
}

func (so *SlidingWindowIncAggOp) emit(ctx api.StreamContext, errCh chan<- error, window *IncAggWindow, now time.Time) {
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
	so.Broadcast(results)
	so.onSend(ctx, results)
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
	FirstTimer *clock.Timer
	ticker     *clock.Ticker
	Length     time.Duration
	Interval   time.Duration
	taskCh     chan *IncAggOpTask
	HoppingWindowIncAggOpState
}

type HoppingWindowIncAggOpState struct {
	CurrWindowList []*IncAggWindow
}

func NewHoppingWindowIncAggOp(o *WindowIncAggOperator) *HoppingWindowIncAggOp {
	op := &HoppingWindowIncAggOp{
		WindowIncAggOperator: o,
		Length:               o.windowConfig.Length,
		Interval:             o.windowConfig.Interval,
		taskCh:               make(chan *IncAggOpTask, 1024),
	}
	op.HoppingWindowIncAggOpState.CurrWindowList = make([]*IncAggWindow, 0)
	return op
}

func (ho *HoppingWindowIncAggOp) PutState(ctx api.StreamContext) {
	for index, window := range ho.CurrWindowList {
		window.GenerateAllFunctionState()
		ho.CurrWindowList[index] = window
	}
	ctx.PutState(buildStateKey(ctx), ho.HoppingWindowIncAggOpState)
}

func (ho *HoppingWindowIncAggOp) RestoreFromState(ctx api.StreamContext) error {
	s, err := ctx.GetState(buildStateKey(ctx))
	if err != nil {
		return err
	}
	if s == nil {
		return nil
	}
	coState, ok := s.(HoppingWindowIncAggOpState)
	if !ok {
		return fmt.Errorf("not HoppingWindowIncAggOpState")
	}
	ho.HoppingWindowIncAggOpState = coState
	for index, window := range ho.CurrWindowList {
		window.restoreState(ctx)
		ho.CurrWindowList[index] = window
	}
	now := time.Now()
	ho.CurrWindowList = gcIncAggWindow(ho.CurrWindowList, ho.Length, now)
	for _, window := range ho.CurrWindowList {
		go func(restoreWindow *IncAggWindow) {
			after := timex.After(now.Sub(restoreWindow.StartTime))
			select {
			case <-ctx.Done():
				return
			case <-after:
				ho.taskCh <- &IncAggOpTask{window: restoreWindow}
			}
		}(window)
	}
	return nil
}

func (ho *HoppingWindowIncAggOp) exec(ctx api.StreamContext, errCh chan<- error) {
	if err := ho.RestoreFromState(ctx); err != nil {
		errCh <- err
		return
	}
	defer func() {
		if ho.ticker != nil {
			ho.ticker.Stop()
		}
	}()
	now := timex.GetNow()
	if !EnableAlignWindow {
		ho.ticker = timex.GetTicker(ho.Interval)
		ho.newIncWindow(ctx, now)
	} else {
		_, ho.FirstTimer = getFirstTimer(ctx, ho.windowConfig.RawInterval, ho.windowConfig.TimeUnit)
		if ho.FirstTimer != nil {
			ho.markFirstTimerCreated()
		}
		ho.CurrWindowList = append(ho.CurrWindowList, newIncAggWindow(ctx, now))
	}
	fv, _ := xsql.NewFunctionValuersForOp(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case done := <-ho.putStateReqCh:
			ho.PutState(ctx)
			done <- nil
		case done := <-ho.restoreReqCh:
			done <- ho.RestoreFromState(ctx)
		case task := <-ho.taskCh:
			now := timex.GetNow()
			ho.emit(ctx, errCh, task.window, now)
			ho.CurrWindowList = gcIncAggWindow(ho.CurrWindowList, ho.Length, now)
			ho.PutState(ctx)
		case input := <-ho.input:
			now := timex.GetNow()
			data, processed := ho.commonIngest(ctx, input)
			if processed {
				continue
			}
			ho.onProcessStart(ctx, input)
			switch row := data.(type) {
			case *xsql.Tuple:
				ho.CurrWindowList = gcIncAggWindow(ho.CurrWindowList, ho.Length, now)
				ho.calIncAggWindow(ctx, fv, row, now)
			}
			ho.PutState(ctx)
		default:
		}
		if ho.FirstTimer != nil {
			select {
			case <-ctx.Done():
				return
			case now := <-ho.FirstTimer.C:
				ho.FirstTimer.Stop()
				ho.FirstTimer = nil
				ho.CurrWindowList = gcIncAggWindow(ho.CurrWindowList, ho.Length, now)
				ho.newIncWindow(ctx, now)
				ho.CurrWindowList = gcIncAggWindow(ho.CurrWindowList, ho.Length, now)
				ho.ticker = timex.GetTicker(ho.Interval)
				ho.PutState(ctx)
			default:
			}
		}
		if ho.ticker != nil {
			select {
			case <-ctx.Done():
				return
			case now := <-ho.ticker.C:
				ho.CurrWindowList = gcIncAggWindow(ho.CurrWindowList, ho.Length, now)
				ho.newIncWindow(ctx, now)
				ho.PutState(ctx)
			default:
			}
		}
	}
}

func (ho *HoppingWindowIncAggOp) newIncWindow(ctx api.StreamContext, now time.Time) {
	newWindow := newIncAggWindow(ctx, now)
	ho.CurrWindowList = append(ho.CurrWindowList, newWindow)
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
		for name, value := range incAggRange.Fields {
			incAggRange.LastRow.Set(name, value)
		}
		results.Content = append(results.Content, incAggRange.LastRow)
	}
	results.WindowRange = xsql.NewWindowRange(window.StartTime.UnixMilli(), now.UnixMilli(), now.UnixMilli())
	ho.Broadcast(results)
	ho.onSend(ctx, results)
}

func (ho *HoppingWindowIncAggOp) calIncAggWindow(ctx api.StreamContext, fv *xsql.FunctionValuer, row *xsql.Tuple, now time.Time) {
	name := calDimension(fv, ho.Dimensions, row)
	for _, incWindow := range ho.CurrWindowList {
		if incWindow.StartTime.Compare(now) <= 0 && incWindow.StartTime.Add(ho.Length).After(now) {
			incAggCal(ctx, name, row, incWindow, ho.aggFields)
		}
	}
}

func incAggCal(ctx api.StreamContext, dimension string, row *xsql.Tuple, incAggWindow *IncAggWindow, aggFields []*ast.Field) {
	dimensionsRange, ok := incAggWindow.DimensionsIncAggRange[dimension]
	if !ok {
		dimensionsRange = newIncAggRange(ctx)
		incAggWindow.DimensionsIncAggRange[dimension] = dimensionsRange
	}
	cloneRow := cloneTuple(row, len(row.Message))
	ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(dimensionsRange.fv, cloneRow, &xsql.WildcardValuer{Data: cloneRow})}
	dimensionsRange.LastRow = cloneRow
	for _, aggField := range aggFields {
		vi := ve.Eval(aggField.Expr)
		colName := aggField.Name
		if len(aggField.AName) > 0 {
			colName = aggField.AName
		}
		dimensionsRange.Fields[colName] = vi
	}
}

func newIncAggRange(ctx api.StreamContext) *IncAggRange {
	fstore, _ := state.CreateStore("incAggWindow", 0)
	fctx := topoContext.Background().WithMeta(ctx.GetRuleId(), ctx.GetOpId(), fstore)
	fv, _ := xsql.NewFunctionValuersForOp(fctx)
	return &IncAggRange{
		fctx:   fctx.(*topoContext.DefaultContext),
		fv:     fv,
		Fields: make(map[string]interface{}),
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
	if dimensions == nil {
		return name
	}
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

func buildStateKey(ctx api.StreamContext) string {
	return fmt.Sprintf("%v_%v_%v/state", ctx.GetRuleId(), ctx.GetOpId(), ctx.GetInstanceId())
}
