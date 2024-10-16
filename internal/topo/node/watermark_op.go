// Copyright 2023-2024 EMQ Technologies Co., Ltd.
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
	"sort"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"go.opentelemetry.io/otel/trace"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

// WatermarkOp is used when event time is enabled.
// It is used to align the event time of the input streams
// It sends out the data in time order with watermark.
type WatermarkOp struct {
	*defaultSinkNode
	// config
	lateTolerance time.Duration
	sendWatermark bool
	// state
	events          []*xsql.Tuple // All the cached events in order
	rowHandle       map[any]trace.Span
	streamWMs       map[string]time.Time
	lastWatermarkTs time.Time
}

var _ OperatorNode = &WatermarkOp{}

const (
	WatermarkKey  = "$$wartermark"
	EventInputKey = "$$eventinputs"
	StreamWMKey   = "$$streamwms"
)

func NewWatermarkOp(name string, sendWatermark bool, streams []string, options *def.RuleOption) *WatermarkOp {
	wms := make(map[string]time.Time, len(streams))
	for _, s := range streams {
		wms[s] = time.Time{}.Add(time.Duration(options.LateTol))
	}
	return &WatermarkOp{
		defaultSinkNode: newDefaultSinkNode(name, options),
		lateTolerance:   time.Duration(options.LateTol),
		sendWatermark:   sendWatermark,
		streamWMs:       wms,
		lastWatermarkTs: time.Time{},
		rowHandle:       make(map[any]trace.Span),
	}
}

func (w *WatermarkOp) Exec(ctx api.StreamContext, errCh chan<- error) {
	w.prepareExec(ctx, errCh, "op")
	// restore state
	if s, err := ctx.GetState(WatermarkKey); err == nil && s != nil {
		if si, ok := s.(time.Time); ok {
			w.lastWatermarkTs = si
		} else {
			infra.DrainError(ctx, fmt.Errorf("restore watermark state `lastWatermarkTs` %v error, invalid type", s), errCh)
			return
		}
	}
	if s, err := ctx.GetState(EventInputKey); err == nil {
		switch st := s.(type) {
		case []*xsql.Tuple:
			w.events = st
			ctx.GetLogger().Infof("Restore watermark events state %+v", st)
		case nil:
			ctx.GetLogger().Debugf("Restore watermark events state, nothing")
		default:
			infra.DrainError(ctx, fmt.Errorf("restore watermark event state %v error, invalid type", st), errCh)
			return
		}
	} else {
		ctx.GetLogger().Warnf("Restore watermark event state fails: %s", err)
	}
	if s, err := ctx.GetState(StreamWMKey); err == nil && s != nil {
		if si, ok := s.(map[string]time.Time); ok {
			w.streamWMs = si
		} else {
			infra.DrainError(ctx, fmt.Errorf("restore watermark stream keys state %v error, invalid type", s), errCh)
			return
		}
	}

	ctx.GetLogger().Infof("Start with state lastWatermarkTs: %d", w.lastWatermarkTs.UnixMilli())
	go func() {
		defer func() {
			w.Close()
		}()
		err := infra.SafeRun(func() error {
			for {
				select {
				case <-ctx.Done():
					ctx.GetLogger().Infof("watermark node %s is finished", w.name)
					return nil
				case item := <-w.input:
					data, processed := w.commonIngest(ctx, item)
					if processed {
						break
					}
					w.onProcessStart(ctx, data)
					if w.span != nil {
						w.rowHandle[data] = w.span
					}
					switch d := data.(type) {
					case *xsql.Tuple:
						// whether to drop the late event
						if w.track(ctx, d.Emitter, d.Timestamp) {
							// If not drop, check if it can be sent out
							w.addAndTrigger(ctx, d)
						}
					default:
						w.onError(ctx, fmt.Errorf("run watermark op error: expect *xsql.Tuple type but got %[1]T(%[1]v)", d))
					}
					w.span = nil
					w.onProcessEnd(ctx)
				}
			}
		})
		if err != nil {
			infra.DrainError(ctx, err, errCh)
		}
	}()
}

func (w *WatermarkOp) track(ctx api.StreamContext, emitter string, ts time.Time) bool {
	ctx.GetLogger().Debugf("watermark generator track event from topic %s at %d", emitter, ts.UnixMilli())
	watermark, ok := w.streamWMs[emitter]
	if !ok || ts.After(watermark) {
		w.streamWMs[emitter] = ts
		_ = ctx.PutState(StreamWMKey, w.streamWMs)
	}
	r := ts.After(w.lastWatermarkTs) || ts.Equal(w.lastWatermarkTs)
	return r
}

// Add an event and check if watermark proceeds
// If yes, send out all events before the watermark
func (w *WatermarkOp) addAndTrigger(ctx api.StreamContext, d *xsql.Tuple) {
	// Insert into the sorted array, should be faster than append then sort
	if len(w.events) == 0 {
		w.events = append(w.events, d)
	} else {
		index := sort.Search(len(w.events), func(i int) bool {
			return w.events[i].Timestamp.After(d.Timestamp)
		})
		w.events = append(w.events, nil)
		copy(w.events[index+1:], w.events[index:])
		w.events[index] = d
	}

	watermark := w.computeWatermarkTs()
	ctx.GetLogger().Debugf("compute watermark event at %d with last %d", watermark.UnixMilli(), w.lastWatermarkTs.UnixMilli())
	// Make sure watermark time proceeds
	if watermark.After(w.lastWatermarkTs) {
		// Send out all events before the watermark
		if watermark.After(w.events[0].Timestamp) || watermark.Equal(w.events[0].Timestamp) {
			// Find out the last event to send in this watermark change
			c := len(w.events)
			for i, e := range w.events {
				if e.Timestamp.After(watermark) {
					c = i
					break
				}
			}

			// Send out all events before the watermark
			for i := 0; i < c; i++ {
				if i > 0 { // The first event processing time start at the beginning of event receiving
					w.statManager.ProcessTimeStart()
				}
				span, stored := w.rowHandle[w.events[i]]
				if stored {
					// set the current span which will be set in broadcast
					w.span = span
				}
				w.Broadcast(w.events[i])
				if stored {
					span.End()
					w.span = nil
				}
				w.onSend(ctx, w.events[i])
				ctx.GetLogger().Debug("send out event", w.events[i].GetTimestamp())
			}
			w.events = w.events[c:]
			_ = ctx.PutState(EventInputKey, w.events)
		}
		// Update watermark
		if w.sendWatermark {
			w.Broadcast(&xsql.WatermarkTuple{Timestamp: watermark})
		}
		w.lastWatermarkTs = watermark
		_ = ctx.PutState(WatermarkKey, w.lastWatermarkTs)
		ctx.GetLogger().Debugf("scan watermark event at %d", watermark.UnixMilli())
	}
}

// watermark is the minimum timestamp of all input topics
func (w *WatermarkOp) computeWatermarkTs() time.Time {
	ts := timex.Maxtime
	for _, wm := range w.streamWMs {
		if ts.After(wm) {
			ts = wm
		}
	}
	return ts.Add(-w.lateTolerance)
}
