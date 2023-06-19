// Copyright 2021-2023 EMQ Technologies Co., Ltd.
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
	"math"
	"time"

	"github.com/benbjohnson/clock"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/node/metric"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/infra"
)

type WindowConfig struct {
	Type        ast.WindowType
	Length      int64
	Interval    int64 // If the interval is not set, it is equals to Length
	Delay       int64
	RawInterval int
	TimeUnit    ast.Token
}

type WindowOperator struct {
	*defaultSinkNode
	window      *WindowConfig
	interval    int64
	isEventTime bool
	trigger     *EventTimeTrigger // For event time only

	statManager metric.StatManager
	ticker      *clock.Ticker // For processing time only
	// states
	triggerTime int64
	msgCount    int
}

const (
	WindowInputsKey = "$$windowInputs"
	TriggerTimeKey  = "$$triggerTime"
	MsgCountKey     = "$$msgCount"
)

func init() {
	gob.Register([]*xsql.Tuple{})
	gob.Register([]map[string]interface{}{})
}

func NewWindowOp(name string, w WindowConfig, options *api.RuleOption) (*WindowOperator, error) {
	o := new(WindowOperator)

	o.defaultSinkNode = &defaultSinkNode{
		input: make(chan interface{}, options.BufferLength),
		defaultNode: &defaultNode{
			outputs:   make(map[string]chan<- interface{}),
			name:      name,
			sendError: options.SendError,
		},
	}
	o.isEventTime = options.IsEventTime
	o.window = &w
	if o.window.Interval == 0 && o.window.Type == ast.COUNT_WINDOW {
		// if no interval value is set, and it's a count window, then set interval to length value.
		o.window.Interval = o.window.Length
	}
	if options.IsEventTime {
		// Create watermark generator
		if w, err := NewEventTimeTrigger(o.window); err != nil {
			return nil, err
		} else {
			o.trigger = w
		}
	}
	return o, nil
}

// Exec is the entry point for the executor
// input: *xsql.Tuple from preprocessor
// output: xsql.WindowTuplesSet
func (o *WindowOperator) Exec(ctx api.StreamContext, errCh chan<- error) {
	o.ctx = ctx
	log := ctx.GetLogger()
	log.Debugf("Window operator %s is started", o.name)

	if len(o.outputs) <= 0 {
		infra.DrainError(ctx, fmt.Errorf("no output channel found"), errCh)
		return
	}
	stats, err := metric.NewStatManager(ctx, "op")
	if err != nil {
		infra.DrainError(ctx, err, errCh)
		return
	}
	o.statManager = stats
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
		o.triggerTime = conf.GetNowInMilli()
	}
	if s, err := ctx.GetState(TriggerTimeKey); err == nil && s != nil {
		if si, ok := s.(int64); ok {
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
	log.Infof("Start with window state triggerTime: %d, msgCount: %d", o.triggerTime, o.msgCount)
	if o.isEventTime {
		go func() {
			err := infra.SafeRun(func() error {
				o.execEventWindow(ctx, inputs, errCh)
				return nil
			})
			if err != nil {
				infra.DrainError(ctx, err, errCh)
			}
		}()
	} else {
		go func() {
			err := infra.SafeRun(func() error {
				o.execProcessingWindow(ctx, inputs, errCh)
				return nil
			})
			if err != nil {
				infra.DrainError(ctx, err, errCh)
			}
		}()
	}
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

func getFirstTimer(ctx api.StreamContext, rawInerval int, timeUnit ast.Token) (int64, *clock.Timer) {
	next := getAlignedWindowEndTime(conf.GetNow(), rawInerval, timeUnit)
	ctx.GetLogger().Infof("align window timer to %v(%d)", next, next.UnixMilli())
	return next.UnixMilli(), conf.GetTimerByTime(next)
}

func (o *WindowOperator) execProcessingWindow(ctx api.StreamContext, inputs []*xsql.Tuple, errCh chan<- error) {
	log := ctx.GetLogger()
	var (
		timeoutTicker *clock.Timer
		// The first ticker to align the first window to the nature time
		firstTicker *clock.Timer
		firstTime   int64
		nextTime    int64
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

	if firstTicker != nil {
		firstC = firstTicker.C
		// resume the previous window
		if len(inputs) > 0 && o.triggerTime > 0 {
			nextTick := conf.GetNowInMilli() + o.interval
			next := o.triggerTime
			switch o.window.Type {
			case ast.TUMBLING_WINDOW, ast.HOPPING_WINDOW:
				for {
					next = next + o.interval
					if next > nextTick {
						break
					}
					log.Debugf("triggered by restore inputs")
					inputs = o.scan(inputs, next, ctx)
					_ = ctx.PutState(WindowInputsKey, inputs)
					_ = ctx.PutState(TriggerTimeKey, o.triggerTime)
				}
			case ast.SESSION_WINDOW:
				timeout, duration := o.window.Interval, o.window.Length
				for {
					et := inputs[0].Timestamp
					tick := et + (duration - et%duration)
					if et%duration == 0 {
						tick = et
					}
					var p int64
					for _, tuple := range inputs {
						var r int64 = math.MaxInt64
						if p > 0 {
							if tuple.Timestamp-p > timeout {
								r = p + timeout
							}
						}
						if tuple.Timestamp > tick {
							if tick-duration > et && tick < r {
								r = tick
							}
							tick += duration
						}
						if r < math.MaxInt64 {
							next = r
							break
						}
						p = tuple.Timestamp
					}
					if next > nextTick {
						break
					}
					log.Debugf("triggered by restore inputs")
					inputs = o.scan(inputs, next, ctx)
					_ = ctx.PutState(WindowInputsKey, inputs)
					_ = ctx.PutState(TriggerTimeKey, o.triggerTime)
				}
			}
		}
	}
	delayCh := make(chan int64, 100)
	for {
		select {
		case delayTS := <-delayCh:
			o.statManager.ProcessTimeStart()
			inputs = o.scan(inputs, delayTS, ctx)
			o.statManager.ProcessTimeEnd()
			o.statManager.SetBufferLength(int64(len(o.input)))
			_ = ctx.PutState(WindowInputsKey, inputs)
			_ = ctx.PutState(MsgCountKey, o.msgCount)
		// process incoming item
		case item, opened := <-o.input:
			processed := false
			if item, processed = o.preprocess(item); processed {
				break
			}
			o.statManager.IncTotalRecordsIn()
			o.statManager.ProcessTimeStart()
			if !opened {
				o.statManager.IncTotalExceptions("input channel closed")
				break
			}
			switch d := item.(type) {
			case error:
				_ = o.Broadcast(d)
				o.statManager.IncTotalExceptions(d.Error())
			case *xsql.Tuple:
				log.Debugf("Event window receive tuple %s", d.Message)
				inputs = append(inputs, d)
				switch o.window.Type {
				case ast.NOT_WINDOW:
					inputs = o.scan(inputs, d.Timestamp, ctx)
				case ast.SLIDING_WINDOW:
					if o.window.Delay > 0 {
						go func(ts int64) {
							after := time.After(time.Duration(o.window.Delay) * time.Millisecond)
							select {
							case <-after:
								delayCh <- ts
							}
						}(d.Timestamp + o.window.Delay)
					} else {
						inputs = o.scan(inputs, d.Timestamp, ctx)
					}
				case ast.SESSION_WINDOW:
					if timeoutTicker != nil {
						timeoutTicker.Stop()
						timeoutTicker.Reset(time.Duration(o.window.Interval) * time.Millisecond)
					} else {
						timeoutTicker = conf.GetTimer(o.window.Interval)
						timeout = timeoutTicker.C
						o.triggerTime = d.Timestamp
						_ = ctx.PutState(TriggerTimeKey, o.triggerTime)
						log.Debugf("Session window set start time %d", o.triggerTime)
					}
				case ast.COUNT_WINDOW:
					o.msgCount++
					log.Debugf(fmt.Sprintf("msgCount: %d", o.msgCount))
					if int64(o.msgCount)%o.window.Interval != 0 {
						continue
					}
					o.msgCount = 0

					if tl, er := NewTupleList(inputs, int(o.window.Length)); er != nil {
						log.Error(fmt.Sprintf("Found error when trying to "))
						infra.DrainError(ctx, er, errCh)
						return
					} else {
						log.Debugf(fmt.Sprintf("It has %d of count window.", tl.count()))
						triggerTime := conf.GetNowInMilli()
						for tl.hasMoreCountWindow() {
							tsets := tl.nextCountWindow()
							windowStart := triggerTime
							triggerTime = conf.GetNowInMilli()
							windowEnd := triggerTime
							tsets.WindowRange = xsql.NewWindowRange(windowStart, windowEnd)
							log.Debugf("Sent: %v", tsets)
							_ = o.Broadcast(tsets)
							o.statManager.IncTotalRecordsOut()
						}
						inputs = tl.getRestTuples()
					}
				}
				o.statManager.ProcessTimeEnd()
				o.statManager.SetBufferLength(int64(len(o.input)))
				_ = ctx.PutState(WindowInputsKey, inputs)
				_ = ctx.PutState(MsgCountKey, o.msgCount)
			default:
				e := fmt.Errorf("run Window error: expect xsql.Tuple type but got %[1]T(%[1]v)", d)
				_ = o.Broadcast(e)
				o.statManager.IncTotalExceptions(e.Error())
			}
		case now := <-firstC:
			log.Debugf("First tick at %v(%d), defined at %d", now, now.UnixMilli(), firstTime)
			switch o.window.Type {
			case ast.TUMBLING_WINDOW:
				o.ticker = conf.GetTicker(o.window.Length)
			case ast.HOPPING_WINDOW:
				o.ticker = conf.GetTicker(o.window.Interval)
			case ast.SESSION_WINDOW:
				o.ticker = conf.GetTicker(o.window.Length)
			}
			firstTicker = nil
			c = o.ticker.C
			inputs = o.tick(ctx, inputs, firstTime, log)
			if o.window.Type == ast.SESSION_WINDOW {
				nextTime = firstTime + o.window.Length
			} else {
				nextTime = firstTime + o.interval
			}
		case now := <-c:
			log.Debugf("Successive tick at %v(%d)", now, now.UnixMilli())
			inputs = o.tick(ctx, inputs, nextTime, log)
			if o.window.Type == ast.SESSION_WINDOW {
				nextTime += o.window.Length
			} else {
				nextTime += o.interval
			}
		case now := <-timeout:
			if len(inputs) > 0 {
				o.statManager.ProcessTimeStart()
				log.Debugf("triggered by timeout")
				inputs = o.scan(inputs, cast.TimeToUnixMilli(now), ctx)
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
			log.Infoln("Cancelling window....")
			if o.ticker != nil {
				o.ticker.Stop()
			}
			return
		}
	}
}

func (o *WindowOperator) tick(ctx api.StreamContext, inputs []*xsql.Tuple, n int64, log api.Logger) []*xsql.Tuple {
	if o.window.Type == ast.SESSION_WINDOW {
		log.Debugf("session window update trigger time %d with %d inputs", n, len(inputs))
		if len(inputs) == 0 || n-o.window.Length < inputs[0].Timestamp {
			if len(inputs) > 0 {
				log.Debugf("session window last trigger time %d < first tuple %d", n-o.window.Length, inputs[0].Timestamp)
			}
			return inputs
		}
	}
	o.statManager.ProcessTimeStart()
	log.Debugf("triggered by ticker at %d", n)
	inputs = o.scan(inputs, n, ctx)
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
		Content: make([]xsql.TupleRow, 0),
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

func (o *WindowOperator) scan(inputs []*xsql.Tuple, triggerTime int64, ctx api.StreamContext) []*xsql.Tuple {
	log := ctx.GetLogger()
	log.Debugf("window %s triggered at %s(%d)", o.name, time.Unix(triggerTime/1000, triggerTime%1000), triggerTime)
	var (
		delta       int64
		windowStart int64
		windowEnd   = triggerTime
	)
	if o.window.Type == ast.HOPPING_WINDOW || o.window.Type == ast.SLIDING_WINDOW {
		delta = o.calDelta(triggerTime, log)
	}
	results := &xsql.WindowTuples{
		Content: make([]xsql.TupleRow, 0),
	}
	i := 0
	length := o.window.Length + o.window.Delay
	// Sync table
	for _, tuple := range inputs {
		if o.window.Type == ast.HOPPING_WINDOW || o.window.Type == ast.SLIDING_WINDOW {
			diff := triggerTime - tuple.Timestamp
			if diff > length+delta {
				log.Debugf("diff: %d, length: %d, delta: %d", diff, length, delta)
				log.Debugf("tuple %s emitted at %d expired", tuple, tuple.Timestamp)
				// Expired tuple, remove it by not adding back to inputs
				continue
			}
			// Added back all inputs for non expired events
			inputs[i] = tuple
			i++
		} else if tuple.Timestamp > triggerTime {
			// Only added back early arrived events
			inputs[i] = tuple
			i++
		}
		if tuple.Timestamp <= triggerTime {
			results = results.AddTuple(tuple)
		}
	}

	switch o.window.Type {
	case ast.TUMBLING_WINDOW, ast.SESSION_WINDOW:
		windowStart = o.triggerTime
	case ast.HOPPING_WINDOW:
		windowStart = o.triggerTime - o.window.Interval
	case ast.SLIDING_WINDOW:
		windowStart = triggerTime - length
	}
	if windowStart <= 0 {
		windowStart = windowEnd - length
	}
	results.WindowRange = xsql.NewWindowRange(windowStart, windowEnd)
	log.Debugf("window %s triggered for %d tuples", o.name, len(inputs))
	log.Debugf("Sent: %v", results)
	_ = o.Broadcast(results)
	o.statManager.IncTotalRecordsOut()

	o.triggerTime = triggerTime
	log.Debugf("new trigger time %d", o.triggerTime)
	return inputs[:i]
}

func (o *WindowOperator) calDelta(triggerTime int64, log api.Logger) int64 {
	var delta int64
	lastTriggerTime := o.triggerTime
	if lastTriggerTime <= 0 {
		delta = math.MaxInt16 // max int, all events for the initial window
	} else {
		if !o.isEventTime && o.window.Interval > 0 {
			delta = triggerTime - lastTriggerTime - o.window.Interval
			if delta > 100 {
				log.Warnf("Possible long computation in window; Previous eviction time: %d, current eviction time: %d", lastTriggerTime, triggerTime)
			}
		} else {
			delta = 0
		}
	}
	return delta
}

func (o *WindowOperator) GetMetrics() [][]interface{} {
	if o.statManager != nil {
		return [][]interface{}{
			o.statManager.GetMetrics(),
		}
	} else {
		return nil
	}
}
