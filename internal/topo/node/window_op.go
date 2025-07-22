// Copyright 2021-2025 EMQ Technologies Co., Ltd.
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
	"math"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/lf-edge/ekuiper/contract/v2/api"
	"go.opentelemetry.io/otel/trace"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	topoContext "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/topo/node/tracenode"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

type WindowConfig struct {
	TriggerCondition ast.Expr
	StateFuncs       []*ast.Call
	Type             ast.WindowType
	// For time window
	Length   time.Duration
	Interval time.Duration // If the interval is not set, it is equals to Length
	Delay    time.Duration
	// For count window
	CountLength   int
	CountInterval int
	RawInterval   int
	TimeUnit      ast.Token

	// For SlidingWindow
	enableSlidingWindowSendTwice bool
}

type WindowOperator struct {
	*defaultSinkNode
	window          *WindowConfig
	interval        time.Duration
	duration        time.Duration
	isEventTime     bool
	isOverlapWindow bool
	trigger         *EventTimeTrigger // For event time only

	ticker *clock.Ticker // For processing time only
	// states
	triggerTime      time.Time
	msgCount         int
	delayTS          []time.Time
	triggerTS        []time.Time
	triggerCondition ast.Expr
	stateFuncs       []*ast.Call

	nextLink     trace.Link
	nextSpanCtx  context.Context
	nextSpan     trace.Span
	tupleSpanMap map[*xsql.Tuple]trace.Span
}

const (
	WindowInputsKey = "$$windowInputs"
	TriggerTimeKey  = "$$triggerTime"
	MsgCountKey     = "$$msgCount"
)

func init() {
	gob.Register([]*xsql.Tuple{})
	gob.Register([]map[string]interface{}{})
	gob.Register(map[string]time.Time{})
}

func NewWindowOp(name string, w WindowConfig, options *def.RuleOption) (*WindowOperator, error) {
	o := new(WindowOperator)

	o.defaultSinkNode = newDefaultSinkNode(name, options)
	o.isEventTime = options.IsEventTime
	w.enableSlidingWindowSendTwice = options.PlanOptimizeStrategy.IsSlidingWindowSendTwiceEnable() && w.Type == ast.SLIDING_WINDOW && w.Delay > 0
	o.window = &w
	if o.window.CountInterval == 0 && o.window.Type == ast.COUNT_WINDOW {
		// if no interval value is set, and it's a count window, then set interval to length value.
		o.window.CountInterval = o.window.CountLength
	}
	if options.IsEventTime {
		// Create watermark generator
		if w, err := NewEventTimeTrigger(o.window); err != nil {
			return nil, err
		} else {
			o.trigger = w
		}
	}
	if w.TriggerCondition != nil {
		o.triggerCondition = w.TriggerCondition
		o.stateFuncs = w.StateFuncs
	}
	o.delayTS = make([]time.Time, 0)
	o.triggerTS = make([]time.Time, 0)
	o.triggerTime = time.Time{}
	o.isOverlapWindow = isOverlapWindow(w.Type)
	o.tupleSpanMap = make(map[*xsql.Tuple]trace.Span)
	return o, nil
}

func (o *WindowOperator) Close() {
	o.defaultNode.Close()
}

// Exec is the entry point for the executor
// input: *xsql.Tuple from preprocessor
// output: xsql.WindowTuplesSet
func (o *WindowOperator) Exec(ctx api.StreamContext, errCh chan<- error) {
	o.prepareExec(ctx, errCh, "op")
	log := ctx.GetLogger()
	var inputs []*xsql.Tuple
	if s, err := ctx.GetState(WindowInputsKey); err == nil {
		switch st := s.(type) {
		case []*xsql.Tuple:
			inputs = st
			log.Infof("Restore window state %+v", inputs)
		case nil:
			log.Debugf("Restore window state, nothing")
		default:
			infra.DrainError(ctx, fmt.Errorf("restore window state `inputs` %v error, invalid type", st), errCh)
			return
		}
	} else {
		log.Warnf("Restore window state fails: %s", err)
	}
	if !o.isEventTime {
		o.triggerTime = timex.GetNow()
	}
	if s, err := ctx.GetState(TriggerTimeKey); err == nil && s != nil {
		if si, ok := s.(time.Time); ok {
			o.triggerTime = si
		} else {
			errCh <- fmt.Errorf("restore window state `triggerTime` %v error, invalid type", s)
		}
	}
	o.msgCount = 0
	if s, err := ctx.GetState(MsgCountKey); err == nil && s != nil {
		if si, ok := s.(int); ok {
			o.msgCount = si
		} else {
			infra.DrainError(ctx, fmt.Errorf("restore window state `msgCount` %v error, invalid type", s), errCh)
			return
		}
	}
	log.Infof("Start with window state triggerTime: %d, msgCount: %d", o.triggerTime.UnixMilli(), o.msgCount)
	o.handleNextWindowTupleSpan(ctx)
	go func() {
		defer func() {
			o.Close()
		}()
		if o.isEventTime {
			err := infra.SafeRun(func() error {
				o.execEventWindow(ctx, inputs, errCh)
				return nil
			})
			if err != nil {
				infra.DrainError(ctx, err, errCh)
			}
		} else {
			err := infra.SafeRun(func() error {
				o.execProcessingWindow(ctx, inputs, errCh)
				return nil
			})
			if err != nil {
				infra.DrainError(ctx, err, errCh)
			}
		}
	}()
}

func getAlignedWindowEndTime(n time.Time, interval int, timeUnit ast.Token) time.Time {
	switch timeUnit {
	case ast.DD: // The interval * days starting today
		return time.Date(n.Year(), n.Month(), n.Day()+interval, 0, 0, 0, 0, n.Location())
	case ast.HH:
		gap := interval
		if n.Hour() > interval {
			gap = interval * (n.Hour()/interval + 1)
		}
		return time.Date(n.Year(), n.Month(), n.Day(), 0, 0, 0, 0, n.Location()).Add(time.Duration(gap) * time.Hour)
	case ast.MI:
		gap := interval
		if n.Minute() > interval {
			gap = interval * (n.Minute()/interval + 1)
		}
		return time.Date(n.Year(), n.Month(), n.Day(), n.Hour(), 0, 0, 0, n.Location()).Add(time.Duration(gap) * time.Minute)
	case ast.SS:
		gap := interval
		if n.Second() > interval {
			gap = interval * (n.Second()/interval + 1)
		}
		return time.Date(n.Year(), n.Month(), n.Day(), n.Hour(), n.Minute(), 0, 0, n.Location()).Add(time.Duration(gap) * time.Second)
	case ast.MS:
		milli := n.Nanosecond() / int(time.Millisecond)
		gap := interval
		if milli > interval {
			gap = interval * (milli/interval + 1)
		}
		return time.Date(n.Year(), n.Month(), n.Day(), n.Hour(), n.Minute(), n.Second(), 0, n.Location()).Add(time.Duration(gap) * time.Millisecond)
	default: // should never happen
		conf.Log.Errorf("invalid time unit %s", timeUnit)
		return n
	}
}

func getFirstTimer(ctx api.StreamContext, rawInerval int, timeUnit ast.Token) (time.Time, *clock.Timer) {
	next := getAlignedWindowEndTime(timex.GetNow(), rawInerval, timeUnit)
	ctx.GetLogger().Infof("align window timer to %v(%d)", next, next.UnixMilli())
	return next, timex.GetTimerByTime(next)
}

func (o *WindowOperator) execProcessingWindow(ctx api.StreamContext, inputs []*xsql.Tuple, errCh chan<- error) {
	log := ctx.GetLogger()
	var (
		timeoutTicker *clock.Timer
		// The first ticker to align the first window to the nature time
		firstTicker *clock.Timer
		firstTime   time.Time
		nextTime    time.Time
		firstC      <-chan time.Time
		timeout     <-chan time.Time
		c           <-chan time.Time
	)
	switch o.window.Type {
	case ast.NOT_WINDOW:
	case ast.TUMBLING_WINDOW:
		firstTime, firstTicker = getFirstTimer(ctx, o.window.RawInterval, o.window.TimeUnit)
		o.interval = o.window.Length
	case ast.HOPPING_WINDOW:
		firstTime, firstTicker = getFirstTimer(ctx, o.window.RawInterval, o.window.TimeUnit)
		o.interval = o.window.Interval
	case ast.SLIDING_WINDOW:
		o.interval = o.window.Length
	case ast.SESSION_WINDOW:
		firstTime, firstTicker = getFirstTimer(ctx, o.window.RawInterval, o.window.TimeUnit)
		o.interval = o.window.Interval
	case ast.COUNT_WINDOW:
		o.interval = o.window.Interval
	}
	o.duration = o.interval
	if o.window.Type == ast.SESSION_WINDOW {
		o.duration = o.window.Length
	}

	if firstTicker != nil {
		firstC = firstTicker.C
		// resume the previous window
		if len(inputs) > 0 && !o.triggerTime.IsZero() {
			nextTick := timex.GetNow().Add(o.interval)
			next := o.triggerTime
			switch o.window.Type {
			case ast.TUMBLING_WINDOW, ast.HOPPING_WINDOW:
				for {
					next = next.Add(o.interval)
					if next.After(nextTick) {
						break
					}
					log.Debugf("triggered by restore inputs")
					inputs = o.scan(inputs, next, ctx, o.window.Length+o.window.Delay, true)
					_ = ctx.PutState(WindowInputsKey, inputs)
					_ = ctx.PutState(TriggerTimeKey, o.triggerTime)
				}
			case ast.SESSION_WINDOW:
				timeout, duration := o.window.Interval, o.window.Length
				for {
					et := inputs[0].Timestamp
					d := time.Duration(et.UnixMilli()%duration.Milliseconds()) * time.Millisecond
					tick := et.Add(duration - d)
					if d == 0 {
						tick = et
					}
					p := time.Time{}
					for _, tuple := range inputs {
						r := timex.Maxtime
						if !p.IsZero() {
							if tuple.Timestamp.Sub(p) > timeout {
								r = p.Add(timeout)
							}
						}
						if tuple.Timestamp.After(tick) {
							if tick.Sub(et) > duration && tick.Before(r) {
								r = tick
							}
							tick = tick.Add(duration)
						}
						if r.Before(timex.Maxtime) {
							next = r
							break
						}
						p = tuple.Timestamp
					}
					if next.After(nextTick) {
						break
					}
					log.Debugf("triggered by restore inputs")
					inputs = o.scan(inputs, next, ctx, o.window.Length+o.window.Delay, true)
					_ = ctx.PutState(WindowInputsKey, inputs)
					_ = ctx.PutState(TriggerTimeKey, o.triggerTime)
				}
			}
		}
	}
	delayCh := make(chan time.Time, 100)
	for {
		select {
		case delayTS := <-delayCh:
			o.statManager.ProcessTimeStart()
			if o.window.enableSlidingWindowSendTwice {
				// send the last part
				inputs = o.scan(inputs, delayTS, ctx, o.window.Delay, false)
			} else {
				inputs = o.scan(inputs, delayTS, ctx, o.window.Delay+o.window.Length, true)
			}
			o.statManager.ProcessTimeEnd()
			_ = ctx.PutState(WindowInputsKey, inputs)
			_ = ctx.PutState(MsgCountKey, o.msgCount)
		// process incoming item
		case item := <-o.input:
			data, processed := o.commonIngest(ctx, item)
			if processed {
				break
			}
			o.onProcessStart(ctx, data)
			switch d := data.(type) {
			case *xsql.Tuple:
				log.Debugf("Event window receive tuple %s", d.Message)
				o.handleTraceIngestTuple(ctx, d)
				inputs = append(inputs, d)
				switch o.window.Type {
				case ast.NOT_WINDOW:
					inputs = o.scan(inputs, d.Timestamp, ctx, o.window.Length+o.window.Delay, true)
				case ast.SLIDING_WINDOW:
					if o.isMatchCondition(ctx, d) {
						if o.window.Delay > 0 {
							if o.window.enableSlidingWindowSendTwice {
								// send the first part
								inputs = o.scan(inputs, d.Timestamp, ctx, o.window.Length, true)
							}
							go func(ts time.Time) {
								after := timex.After(o.window.Delay)
								select {
								case <-after:
									delayCh <- ts
								case <-ctx.Done():
									return
								}
							}(d.Timestamp.Add(o.window.Delay))
						} else {
							inputs = o.scan(inputs, d.Timestamp, ctx, o.window.Length+o.window.Delay, true)
						}
					} else {
						// clear inputs if condition not matched
						// TS add 1 to prevent remove current input
						inputs = o.gcInputs(inputs, d.Timestamp.Add(1), ctx)
					}
				case ast.SESSION_WINDOW:
					if timeoutTicker != nil {
						timeoutTicker.Stop()
						timeoutTicker.Reset(o.window.Interval)
					} else {
						timeoutTicker = timex.GetTimer(o.window.Interval)
						timeout = timeoutTicker.C
						o.triggerTime = d.Timestamp
						_ = ctx.PutState(TriggerTimeKey, o.triggerTime)
						log.Debugf("Session window set start time %d", o.triggerTime.UnixMilli())
					}
				case ast.COUNT_WINDOW:
					o.msgCount++
					log.Debugf(fmt.Sprintf("msgCount: %d", o.msgCount))
					if o.msgCount%o.window.CountInterval != 0 {
						continue
					}
					o.msgCount = 0

					if tl, er := NewTupleList(inputs, o.window.CountLength); er != nil {
						log.Error(fmt.Sprintf("Found error when trying to "))
						infra.DrainError(ctx, er, errCh)
						return
					} else {
						log.Debugf(fmt.Sprintf("It has %d of count window.", tl.count()))
						triggerTime := timex.GetNowInMilli()
						for tl.hasMoreCountWindow() {
							tsets := tl.nextCountWindow()
							windowStart := triggerTime
							triggerTime = timex.GetNowInMilli()
							windowEnd := triggerTime
							tsets.WindowRange = xsql.NewWindowRange(windowStart, windowEnd, windowEnd)
							log.Debugf("Sent: %v", tsets)
							o.handleTraceEmitTuple(ctx, tsets)
							o.Broadcast(tsets)
							o.onSend(ctx, tsets)
						}
						inputs = tl.getRestTuples()
					}
				}
				_ = ctx.PutState(WindowInputsKey, inputs)
				_ = ctx.PutState(MsgCountKey, o.msgCount)
			default:
				o.onError(ctx, fmt.Errorf("run Window error: expect xsql.Tuple type but got %[1]T(%[1]v)", d))
			}
			// For batching operator, do not end the span immediately so set it to nil
			o.span = nil
			o.onProcessEnd(ctx)
			o.statManager.SetBufferLength(int64(len(o.input)))
		case now := <-firstC:
			log.Infof("First tick at %v(%d), defined at %d", now, now.UnixMilli(), firstTime.UnixMilli())
			firstTicker.Stop()
			o.setupTicker()
			c = o.ticker.C
			inputs = o.tick(ctx, inputs, firstTime, log)
			nextTime = firstTime
		case now := <-c:
			nextTime = nextTime.Add(o.duration)
			log.Debugf("Successive tick at %v(%d), defined at %d", now, now.UnixMilli(), nextTime.UnixMilli())
			// If the deviation is less than 50ms, then process it. Otherwise, time may change and we'll start a new timer
			if now.Sub(nextTime).Abs() < 50*time.Millisecond {
				inputs = o.tick(ctx, inputs, nextTime, log)
			} else {
				log.Infof("Skip the tick at %v(%d) since it's too late", now, now.UnixMilli())
				o.ticker.Stop()
				firstTime, firstTicker = getFirstTimer(ctx, o.window.RawInterval, o.window.TimeUnit)
				firstC = firstTicker.C
			}
		case now := <-timeout:
			if len(inputs) > 0 {
				o.statManager.ProcessTimeStart()
				log.Debugf("triggered by timeout")
				inputs = o.scan(inputs, now, ctx, o.window.Length+o.window.Delay, true)
				_ = inputs
				// expire all inputs, so that when timer scans there is no item
				inputs = make([]*xsql.Tuple, 0)
				o.statManager.ProcessTimeEnd()
				_ = ctx.PutState(WindowInputsKey, inputs)
				_ = ctx.PutState(TriggerTimeKey, o.triggerTime)
				timeoutTicker = nil
			}
		// is cancelling
		case <-ctx.Done():
			log.Info("Cancelling window....")
			if o.ticker != nil {
				o.ticker.Stop()
			}
			return
		}
		o.statManager.SetBufferLength(int64(len(o.input)))
	}
}

func (o *WindowOperator) setupTicker() {
	switch o.window.Type {
	case ast.TUMBLING_WINDOW:
		o.ticker = timex.GetTicker(o.window.Length)
	case ast.HOPPING_WINDOW:
		o.ticker = timex.GetTicker(o.window.Interval)
	case ast.SESSION_WINDOW:
		o.ticker = timex.GetTicker(o.window.Length)
	}
}

func (o *WindowOperator) tick(ctx api.StreamContext, inputs []*xsql.Tuple, n time.Time, log api.Logger) []*xsql.Tuple {
	if o.window.Type == ast.SESSION_WINDOW {
		log.Debugf("session window update trigger time %d with %d inputs", n.UnixMilli(), len(inputs))
		if len(inputs) == 0 || n.Sub(inputs[0].Timestamp) < o.window.Length {
			if len(inputs) > 0 {
				log.Debugf("session window last trigger time %d < first tuple %d", n.Add(-o.window.Length).UnixMilli(), inputs[0].Timestamp.UnixMilli())
			}
			return inputs
		}
	}
	o.statManager.ProcessTimeStart()
	log.Debugf("triggered by ticker at %d", n.UnixMilli())
	inputs = o.scan(inputs, n, ctx, o.window.Length+o.window.Delay, true)
	o.statManager.ProcessTimeEnd()
	_ = ctx.PutState(WindowInputsKey, inputs)
	_ = ctx.PutState(TriggerTimeKey, o.triggerTime)
	return inputs
}

type TupleList struct {
	tuples []*xsql.Tuple
	index  int // Current index
	size   int // The size for count window
}

func NewTupleList(tuples []*xsql.Tuple, windowSize int) (TupleList, error) {
	if windowSize <= 0 {
		return TupleList{}, fmt.Errorf("Window size should not be less than zero.")
	} else if tuples == nil || len(tuples) == 0 {
		return TupleList{}, fmt.Errorf("The tuples should not be nil or empty.")
	}
	tl := TupleList{tuples: tuples, size: windowSize}
	return tl, nil
}

func (tl *TupleList) hasMoreCountWindow() bool {
	if len(tl.tuples) < tl.size {
		return false
	}
	return tl.index == 0
}

func (tl *TupleList) count() int {
	if len(tl.tuples) < tl.size {
		return 0
	} else {
		return 1
	}
}

func (tl *TupleList) nextCountWindow() *xsql.WindowTuples {
	results := &xsql.WindowTuples{
		Content: make([]xsql.Row, 0),
	}
	var subT []*xsql.Tuple
	subT = tl.tuples[len(tl.tuples)-tl.size : len(tl.tuples)]
	for _, tuple := range subT {
		results = results.AddTuple(tuple)
	}
	tl.index = tl.index + 1
	return results
}

func (tl *TupleList) getRestTuples() []*xsql.Tuple {
	if len(tl.tuples) < tl.size {
		return tl.tuples
	}
	return tl.tuples[len(tl.tuples)-tl.size+1:]
}

func (o *WindowOperator) isTimeRelatedWindow() bool {
	switch o.window.Type {
	case ast.SLIDING_WINDOW:
		return o.window.Delay > 0
	case ast.TUMBLING_WINDOW:
		return true
	case ast.HOPPING_WINDOW:
		return true
	case ast.SESSION_WINDOW:
		return true
	}
	return false
}

func isOverlapWindow(winType ast.WindowType) bool {
	switch winType {
	case ast.HOPPING_WINDOW, ast.SLIDING_WINDOW:
		return true
	default:
		return false
	}
}

func (o *WindowOperator) handleInputsForSlidingWindow(ctx api.StreamContext, inputs []*xsql.Tuple, windowStart, windowEnd time.Time) ([]*xsql.Tuple, []*xsql.Tuple, []xsql.Row) {
	log := ctx.GetLogger()
	log.Debugf("window %s triggered at %s(%d)", o.name, windowEnd, windowEnd.UnixMilli())
	var delta time.Duration
	delta = o.calDelta(windowEnd, log)
	content := make([]xsql.Row, 0, len(inputs))
	discardedLeft := windowEnd.Add(-(o.window.Length + o.window.Delay)).Add(-delta)
	log.Debugf("triggerTime: %d, length: %d, delta: %d, leftmost: %d", windowEnd.UnixMilli(), windowEnd.Sub(windowStart), delta, discardedLeft.UnixMilli())
	nextleft := -1
	for i, tuple := range inputs {
		if discardedLeft.After(tuple.Timestamp) {
			nextleft = i
			continue
		}
		if tuple.Timestamp.After(windowStart) {
			if tuple.Timestamp.Before(windowEnd) || tuple.Timestamp.Equal(windowEnd) {
				content = append(content, tuple)
			}
		}
	}
	if nextleft == -1 {
		return inputs, inputs[:0], content
	}
	if nextleft == len(inputs)-1 {
		return inputs[:0], inputs, content
	}
	return inputs[:nextleft+1], inputs[nextleft:], content
}

func (o *WindowOperator) handleInputs(ctx api.StreamContext, inputs []*xsql.Tuple, right time.Time) ([]*xsql.Tuple, []*xsql.Tuple, []xsql.Row) {
	log := ctx.GetLogger()
	log.Debugf("window %s triggered at %s(%d)", o.name, right, right.UnixMilli())
	var delta time.Duration
	length := o.window.Length + o.window.Delay
	if o.window.Type == ast.HOPPING_WINDOW || o.window.Type == ast.SLIDING_WINDOW {
		delta = o.calDelta(right, log)
	}
	content := make([]xsql.Row, 0, len(inputs))
	// Sync table
	left := right.Add(-length).Add(-delta)
	log.Debugf("triggerTime: %d, length: %d, delta: %d, leftmost: %d", right.UnixMilli(), length, delta, left.UnixMilli())
	nextleft := -1
	// this is to avoid always scan all tuples. better for performance if a window is big.
	allDiscarded := false
	// Assume the inputs are sorted by timestamp
	for i, tuple := range inputs {
		// Other window always discard the tuples that has been triggered.
		// So the tuple in the inputs should all bigger than the current left (in the window)
		// For hopping and sliding window, firstly check if the beginning tuples are expired and discard them
		if o.isOverlapWindow && !allDiscarded {
			if left.After(tuple.Timestamp) {
				log.Debugf("tuple %x emitted at %d expired", tuple, tuple.Timestamp.UnixMilli())
				// Expired tuple, remove it by not adding back to inputs
				continue
			}
		}
		allDiscarded = true
		// Now all tuples are in the window. Next step is to check if the tuple is in the current window
		// If the tuple is beyond the right boundary, then it should be in the next window
		meet := tuple.Timestamp.Before(right) || tuple.Timestamp.Equal(right)
		if o.isTimeRelatedWindow() {
			meet = tuple.Timestamp.Before(right)
		}
		if meet {
			content = append(content, tuple)
			if nextleft < 0 && o.isOverlapWindow {
				nextleft = i
			}
		} else {
			if nextleft < 0 && !o.isOverlapWindow {
				nextleft = i
			}
		}
	}
	if nextleft < 0 {
		return inputs[:0], inputs, content
	}
	ctx.GetLogger().Debugf("discard before %d", nextleft)
	return inputs[nextleft:], inputs[:nextleft], content
}

func (o *WindowOperator) gcInputs(inputs []*xsql.Tuple, triggerTime time.Time, ctx api.StreamContext) []*xsql.Tuple {
	length := o.window.Length + o.window.Delay
	gcIndex := -1
	for i, tuple := range inputs {
		if tuple.Timestamp.Add(length).Compare(triggerTime) >= 0 {
			break
		}
		gcIndex = i
	}
	if gcIndex == len(inputs)-1 {
		return inputs[:0]
	}
	if gcIndex == -1 {
		return inputs
	}
	return inputs[gcIndex+1:]
}

func (o *WindowOperator) scan(inputs []*xsql.Tuple, triggerTime time.Time, ctx api.StreamContext, length time.Duration, isFirstPart bool) []*xsql.Tuple {
	log := ctx.GetLogger()
	log.Debugf("window %s triggered at %s(%d)", o.name, triggerTime, triggerTime.UnixMilli())
	var (
		windowStart int64
		windowEnd   = triggerTime
	)
	var discarded []*xsql.Tuple
	var content []xsql.Row
	if o.window.enableSlidingWindowSendTwice {
		inputs, discarded, content = o.handleInputsForSlidingWindow(ctx, inputs, triggerTime.Add(-length), triggerTime)
	} else {
		inputs, discarded, content = o.handleInputs(ctx, inputs, triggerTime)
	}
	results := &xsql.WindowTuples{
		Content: content,
	}
	o.handleTraceEmitTuple(ctx, results)
	o.handleTraceDiscardTuple(ctx, discarded)
	switch o.window.Type {
	case ast.TUMBLING_WINDOW, ast.SESSION_WINDOW:
		windowStart = o.triggerTime.UnixMilli()
	case ast.HOPPING_WINDOW:
		windowStart = (o.triggerTime.Add(-o.window.Interval)).UnixMilli()
	case ast.SLIDING_WINDOW:
		windowStart = triggerTime.Add(-length).UnixMilli()
	}
	if windowStart <= 0 {
		windowStart = windowEnd.Add(-length).UnixMilli()
	}
	if isFirstPart {
		results.WindowRange = xsql.NewWindowRange(windowStart, windowEnd.UnixMilli(), windowEnd.UnixMilli())
	} else if o.window.enableSlidingWindowSendTwice {
		results.WindowRange = xsql.NewWindowRange(windowStart, windowEnd.UnixMilli(), triggerTime.Add(-o.window.Delay).UnixMilli())
	}
	log.Debugf("window %s triggered for %d tuples", o.name, len(inputs))
	log.Debugf("Sent: %v", results)
	o.Broadcast(results)
	o.onSend(ctx, results)

	o.triggerTime = triggerTime
	log.Debugf("new trigger time %d", o.triggerTime.UnixMilli())
	return inputs
}

func (o *WindowOperator) calDelta(triggerTime time.Time, log api.Logger) time.Duration {
	var delta time.Duration
	lastTriggerTime := o.triggerTime
	if lastTriggerTime.IsZero() {
		delta = math.MaxInt16 // max int, all events for the initial window
	} else {
		if !o.isEventTime && o.window.Interval > 0 {
			delta = triggerTime.Sub(lastTriggerTime) - o.window.Interval
			if delta > 100 {
				log.Warnf("Possible long computation in window; Previous eviction time: %d, current eviction time: %d", lastTriggerTime.UnixMilli(), triggerTime.UnixMilli())
			}
		} else {
			delta = 0
		}
	}
	return delta
}

func (o *WindowOperator) isMatchCondition(ctx api.StreamContext, d *xsql.Tuple) bool {
	if o.triggerCondition == nil || o.window.Type != ast.SLIDING_WINDOW {
		return true
	}
	log := ctx.GetLogger()
	fv, _ := xsql.NewFunctionValuersForOp(ctx)
	ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(d, fv)}
	result := ve.Eval(o.triggerCondition)
	// not match trigger condition
	if result == nil {
		return false
	}
	switch v := result.(type) {
	case error:
		log.Errorf("window %s trigger condition meet error: %v", o.name, v)
		return false
	case bool:
		// match trigger condition
		if v {
			for _, f := range o.stateFuncs {
				_ = ve.Eval(f)
			}
		}
		return v
	default:
		return false
	}
}

func (o *WindowOperator) handleTraceIngestTuple(ctx api.StreamContext, t *xsql.Tuple) {
	if o.span != nil {
		o.tupleSpanMap[t] = o.span
	}
}

func (o *WindowOperator) handleTraceDiscardTuple(ctx api.StreamContext, tuples []*xsql.Tuple) {
	if ctx.IsTraceEnabled() {
		for _, tuple := range tuples {
			span, ok := o.tupleSpanMap[tuple]
			if ok {
				span.End()
				delete(o.tupleSpanMap, tuple)
			}
		}
	}
}

func (o *WindowOperator) handleTraceEmitTuple(ctx api.StreamContext, wt *xsql.WindowTuples) {
	if ctx.IsTraceEnabled() {
		if o.nextSpan == nil {
			o.handleNextWindowTupleSpan(ctx)
		}
		for _, row := range wt.Content {
			t, ok := row.(*xsql.Tuple)
			if ok {
				span, stored := o.tupleSpanMap[t]
				if stored {
					span.AddLink(o.nextLink)
				}
			}
		}
		wt.SetTracerCtx(topoContext.WithContext(o.nextSpanCtx))
		// discard span if windowTuple is empty
		if len(wt.Content) > 0 {
			tracenode.RecordRowOrCollection(wt, o.nextSpan)
			o.nextSpan.End()
		}
		o.handleNextWindowTupleSpan(ctx)
	}
}

func (o *WindowOperator) handleNextWindowTupleSpan(ctx api.StreamContext) {
	traced, spanCtx, span := tracenode.StartTraceBackground(ctx, "window_op")
	if traced {
		o.nextSpanCtx = spanCtx
		o.nextSpan = span
		o.nextLink = trace.Link{
			SpanContext: span.SpanContext(),
		}
	}
}
