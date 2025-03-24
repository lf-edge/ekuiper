// Copyright 2021-2024 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

// EventTimeTrigger scans the input tuples and find out the tuples in the current window
// The inputs are sorted by watermark op
type EventTimeTrigger struct {
	window   *WindowConfig
	interval time.Duration
}

func NewEventTimeTrigger(window *WindowConfig) (*EventTimeTrigger, error) {
	w := &EventTimeTrigger{
		window: window,
	}
	switch window.Type {
	case ast.NOT_WINDOW:
	case ast.TUMBLING_WINDOW:
		w.interval = window.Length
	case ast.HOPPING_WINDOW:
		w.interval = window.Interval
	case ast.SLIDING_WINDOW:
		w.interval = window.Length
	case ast.SESSION_WINDOW:
		// Use timeout to update watermark
		w.interval = window.Interval
	default:
		return nil, fmt.Errorf("unsupported window type %d", window.Type)
	}
	return w, nil
}

// If the window end cannot be determined yet, return max int64 so that it can be recalculated for the next watermark
func (w *EventTimeTrigger) getNextWindow(inputs []*xsql.Tuple, current time.Time, watermark time.Time) time.Time {
	switch w.window.Type {
	case ast.TUMBLING_WINDOW, ast.HOPPING_WINDOW:
		if !current.IsZero() {
			return current.Add(w.interval)
		} else { // first run without a previous window
			nextTs := getEarliestEventTs(inputs, current, watermark)
			if nextTs == timex.Maxtime {
				return nextTs
			}
			return getAlignedWindowEndTime(nextTs, w.window.RawInterval, w.window.TimeUnit)
		}
	case ast.SLIDING_WINDOW:
		nextTs := getEarliestEventTs(inputs, current, watermark)
		return nextTs
	default:
		return timex.Maxtime
	}
}

func (w *EventTimeTrigger) getNextSessionWindow(inputs []*xsql.Tuple, now time.Time) (time.Time, bool) {
	if len(inputs) > 0 {
		timeout, duration := w.window.Interval, w.window.Length
		et := inputs[0].Timestamp
		tick := getAlignedWindowEndTime(et, w.window.RawInterval, w.window.TimeUnit)
		p := time.Time{}
		ticked := false
		for _, tuple := range inputs {
			r := timex.Maxtime
			if !p.IsZero() {
				if tuple.Timestamp.Sub(p) > timeout {
					r = p.Add(timeout)
				}
			}
			if tuple.Timestamp.After(tick) {
				if tick.Add(-duration).After(et) && tick.Before(r) {
					r = tick
					ticked = true
				}
				tick = tick.Add(duration)
			}
			if r.Before(timex.Maxtime) {
				return r, ticked
			}
			p = tuple.Timestamp
		}
		if !p.IsZero() {
			if now.Sub(p) > timeout {
				return p.Add(timeout), ticked
			}
		}
	}
	return timex.Maxtime, false
}

func (o *WindowOperator) execEventWindow(ctx api.StreamContext, inputs []*xsql.Tuple, _ chan<- error) {
	log := ctx.GetLogger()
	nextWindowEndTs := timex.Maxtime
	prevWindowEndTs := time.Time{}
	var lastTicked bool
	for {
		select {
		// process incoming item
		case item := <-o.input:
			data, processed := o.ingest(ctx, item)
			if processed {
				break
			}
			switch d := data.(type) {
			case *xsql.WatermarkTuple:
				ctx.GetLogger().Debug("WatermarkTuple", d.GetTimestamp())
				watermarkTs := d.GetTimestamp()
				if o.window.Type == ast.SLIDING_WINDOW {
					for len(o.delayTS) > 0 && (watermarkTs.After(o.delayTS[0]) || watermarkTs.Equal(o.delayTS[0])) {
						inputs = o.scan(inputs, o.delayTS[0], ctx)
						o.delayTS = o.delayTS[1:]
					}
				}

				windowEndTs := nextWindowEndTs
				ticked := false
				// Session window needs a recalculation of window because its window end depends on the inputs
				if windowEndTs.Equal(timex.Maxtime) || o.window.Type == ast.SESSION_WINDOW || o.window.Type == ast.SLIDING_WINDOW {
					if o.window.Type == ast.SESSION_WINDOW {
						windowEndTs, ticked = o.trigger.getNextSessionWindow(inputs, watermarkTs)
					} else {
						windowEndTs = o.trigger.getNextWindow(inputs, prevWindowEndTs, watermarkTs)
					}
				}
				for !windowEndTs.IsZero() && (windowEndTs.Before(watermarkTs) || windowEndTs.Equal(watermarkTs)) {
					log.Debugf("Current input count %d", len(inputs))
					// scan all events and find out the event in the current window
					if o.window.Type == ast.SESSION_WINDOW && !lastTicked {
						o.triggerTime = inputs[0].Timestamp
					}
					if !windowEndTs.IsZero() {
						if o.window.Type == ast.SLIDING_WINDOW {
							for len(o.triggerTS) > 0 && (o.triggerTS[0].Before(watermarkTs) || o.triggerTS[0].Equal(watermarkTs)) {
								if o.window.Delay > 0 {
									o.delayTS = append(o.delayTS, o.triggerTS[0].Add(o.window.Delay))
								} else {
									inputs = o.scan(inputs, o.triggerTS[0], ctx)
								}
								o.triggerTS = o.triggerTS[1:]
							}
						} else {
							inputs = o.scan(inputs, windowEndTs, ctx)
						}
					}
					prevWindowEndTs = windowEndTs
					lastTicked = ticked
					if o.window.Type == ast.SESSION_WINDOW {
						windowEndTs, ticked = o.trigger.getNextSessionWindow(inputs, watermarkTs)
					} else {
						windowEndTs = o.trigger.getNextWindow(inputs, prevWindowEndTs, watermarkTs)
					}
					log.Debugf("Window end ts %d Watermark ts %d\n", windowEndTs.UnixMilli(), watermarkTs.UnixMilli())
				}
				nextWindowEndTs = windowEndTs
				log.Debugf("next window end %d", nextWindowEndTs.UnixMilli())
			case *xsql.Tuple:
				o.onProcessStart(ctx, d)
				o.handleTraceIngestTuple(ctx, d)
				ctx.GetLogger().Debug("Tuple", d.GetTimestamp())
				log.Debugf("event window receive tuple %s", d.Message)
				// first tuple, set the window start time, which will set to triggerTime
				if o.triggerTime.IsZero() {
					o.triggerTime = d.Timestamp
				}
				if o.window.Type == ast.SLIDING_WINDOW && o.isMatchCondition(ctx, d) {
					o.triggerTS = append(o.triggerTS, d.Timestamp)
				}
				inputs = append(inputs, d)
				o.span = nil
				o.onProcessEnd(ctx)
				_ = ctx.PutState(WindowInputsKey, inputs)
			default:
				o.onError(ctx, fmt.Errorf("run Window error: expect xsql.Event type but got %[1]T(%[1]v)", d))
			}
		// is cancelling
		case <-ctx.Done():
			log.Info("Cancelling window....")
			if o.ticker != nil {
				o.ticker.Stop()
			}
			return
		}
	}
}

func getEarliestEventTs(inputs []*xsql.Tuple, startTs time.Time, endTs time.Time) time.Time {
	minTs := timex.Maxtime
	for _, t := range inputs {
		if t.Timestamp.After(startTs) && (t.Timestamp.Before(endTs) || t.Timestamp.Equal(endTs)) && t.Timestamp.Before(minTs) {
			minTs = t.Timestamp
		}
	}
	return minTs
}

func (o *WindowOperator) ingest(ctx api.StreamContext, item any) (any, bool) {
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
