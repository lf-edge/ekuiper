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
	"fmt"
	"math"
	"time"

	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
)

// EventTimeTrigger scans the input tuples and find out the tuples in the current window
// The inputs are sorted by watermark op
type EventTimeTrigger struct {
	window   *WindowConfig
	interval int64
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
func (w *EventTimeTrigger) getNextWindow(inputs []*xsql.Tuple, current int64, watermark int64) int64 {
	switch w.window.Type {
	case ast.TUMBLING_WINDOW, ast.HOPPING_WINDOW:
		if current > 0 {
			return current + w.interval
		} else { // first run without a previous window
			nextTs := getEarliestEventTs(inputs, current, watermark)
			if nextTs == math.MaxInt64 {
				return nextTs
			}
			return getAlignedWindowEndTime(time.UnixMilli(nextTs), w.window.RawInterval, w.window.TimeUnit).UnixMilli()
		}
	case ast.SLIDING_WINDOW:
		nextTs := getEarliestEventTs(inputs, current, watermark)
		return nextTs
	default:
		return math.MaxInt64
	}
}

func (w *EventTimeTrigger) getNextSessionWindow(inputs []*xsql.Tuple, now int64) (int64, bool) {
	if len(inputs) > 0 {
		timeout, duration := w.window.Interval, w.window.Length
		et := inputs[0].Timestamp
		tick := getAlignedWindowEndTime(time.UnixMilli(et), w.window.RawInterval, w.window.TimeUnit).UnixMilli()
		var p int64
		ticked := false
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
					ticked = true
				}
				tick += duration
			}
			if r < math.MaxInt64 {
				return r, ticked
			}
			p = tuple.Timestamp
		}
		if p > 0 {
			if now-p > timeout {
				return p + timeout, ticked
			}
		}
	}
	return math.MaxInt64, false
}

func (o *WindowOperator) execEventWindow(ctx api.StreamContext, inputs []*xsql.Tuple, _ chan<- error) {
	log := ctx.GetLogger()
	var (
		nextWindowEndTs int64
		prevWindowEndTs int64
		lastTicked      bool
	)
	for {
		select {
		// process incoming item
		case item, opened := <-o.input:
			if !opened {
				o.statManager.IncTotalExceptions("input channel closed")
				break
			}
			if _, ok := item.(*xsql.WatermarkTuple); !ok {
				processed := false
				if item, processed = o.preprocess(item); processed {
					break
				}
			}
			switch d := item.(type) {
			case error:
				o.statManager.IncTotalRecordsIn()
				_ = o.Broadcast(d)
				o.statManager.IncTotalExceptions(d.Error())
			case *xsql.WatermarkTuple:
				ctx.GetLogger().Debug("WatermarkTuple", d.GetTimestamp())
				watermarkTs := d.GetTimestamp()
				windowEndTs := nextWindowEndTs
				ticked := false
				// Session window needs a recalculation of window because its window end depends on the inputs
				if windowEndTs == math.MaxInt64 || o.window.Type == ast.SESSION_WINDOW || o.window.Type == ast.SLIDING_WINDOW {
					if o.window.Type == ast.SESSION_WINDOW {
						windowEndTs, ticked = o.trigger.getNextSessionWindow(inputs, watermarkTs)
					} else {
						windowEndTs = o.trigger.getNextWindow(inputs, prevWindowEndTs, watermarkTs)
					}
				}
				for windowEndTs <= watermarkTs && windowEndTs >= 0 {
					log.Debugf("Current input count %d", len(inputs))
					// scan all events and find out the event in the current window
					if o.window.Type == ast.SESSION_WINDOW && !lastTicked {
						o.triggerTime = inputs[0].Timestamp
					}
					if windowEndTs > 0 {
						inputs = o.scan(inputs, windowEndTs, ctx)
					}
					prevWindowEndTs = windowEndTs
					lastTicked = ticked
					if o.window.Type == ast.SESSION_WINDOW {
						windowEndTs, ticked = o.trigger.getNextSessionWindow(inputs, watermarkTs)
					} else {
						windowEndTs = o.trigger.getNextWindow(inputs, prevWindowEndTs, watermarkTs)
					}
					log.Debugf("Window end ts %d Watermark ts %d\n", windowEndTs, watermarkTs)
				}
				nextWindowEndTs = windowEndTs
				log.Debugf("next window end %d", nextWindowEndTs)
			case *xsql.Tuple:
				ctx.GetLogger().Debug("Tuple", d.GetTimestamp())
				o.statManager.ProcessTimeStart()
				o.statManager.IncTotalRecordsIn()
				log.Debugf("event window receive tuple %s", d.Message)
				// first tuple, set the window start time, which will set to triggerTime
				if o.triggerTime == 0 {
					o.triggerTime = d.Timestamp
				}
				inputs = append(inputs, d)
				o.statManager.ProcessTimeEnd()
				_ = ctx.PutState(WindowInputsKey, inputs)
			default:
				e := fmt.Errorf("run Window error: expect xsql.Event type but got %[1]T(%[1]v)", d)
				_ = o.Broadcast(e)
				o.statManager.IncTotalExceptions(e.Error())
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

func getEarliestEventTs(inputs []*xsql.Tuple, startTs int64, endTs int64) int64 {
	var minTs int64 = math.MaxInt64
	for _, t := range inputs {
		if t.Timestamp > startTs && t.Timestamp <= endTs && t.Timestamp < minTs {
			minTs = t.Timestamp
		}
	}
	return minTs
}
