// Copyright 2023 EMQ Technologies Co., Ltd.
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
	"sort"

	"github.com/lf-edge/ekuiper/internal/topo/node/metric"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/infra"
)

// WatermarkOp is used when event time is enabled.
// It is used to align the event time of the input streams
// It sends out the data in time order with watermark.
type WatermarkOp struct {
	*defaultSinkNode
	statManager metric.StatManager
	// config
	lateTolerance int64
	sendWatermark bool
	// state
	events          []*xsql.Tuple // All the cached events in order
	streamWMs       map[string]int64
	lastWatermarkTs int64
}

var _ OperatorNode = &WatermarkOp{}

const (
	WatermarkKey  = "$$wartermark"
	EventInputKey = "$$eventinputs"
	StreamWMKey   = "$$streamwms"
)

func NewWatermarkOp(name string, sendWatermark bool, streams []string, options *api.RuleOption) *WatermarkOp {
	wms := make(map[string]int64, len(streams))
	for _, s := range streams {
		wms[s] = options.LateTol
	}
	return &WatermarkOp{
		defaultSinkNode: &defaultSinkNode{
			input: make(chan interface{}, options.BufferLength),
			defaultNode: &defaultNode{
				outputs:   make(map[string]chan<- interface{}),
				name:      name,
				sendError: options.SendError,
			},
		},
		lateTolerance: options.LateTol,
		sendWatermark: sendWatermark,
		streamWMs:     wms,
	}
}

func (w *WatermarkOp) Exec(ctx api.StreamContext, errCh chan<- error) {
	ctx.GetLogger().Debugf("watermark node %s is started", w.name)
	if len(w.outputs) <= 0 {
		infra.DrainError(ctx, fmt.Errorf("no output channel found"), errCh)
		return
	}
	stats, err := metric.NewStatManager(ctx, "op")
	if err != nil {
		infra.DrainError(ctx, fmt.Errorf("fail to create stat manager"), errCh)
		return
	}
	w.statManager = stats
	w.ctx = ctx
	// restore state
	if s, err := ctx.GetState(WatermarkKey); err == nil && s != nil {
		if si, ok := s.(int64); ok {
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
		if si, ok := s.(map[string]int64); ok {
			w.streamWMs = si
		} else {
			infra.DrainError(ctx, fmt.Errorf("restore watermark stream keys state %v error, invalid type", s), errCh)
			return
		}
	}

	ctx.GetLogger().Infof("Start with state lastWatermarkTs: %d", w.lastWatermarkTs)
	go func() {
		err := infra.SafeRun(func() error {
			for {
				select {
				case <-ctx.Done():
					ctx.GetLogger().Infof("watermark node %s is finished", w.name)
					return nil
				case item, opened := <-w.input:
					if !opened {
						w.statManager.IncTotalExceptions("input channel closed")
						break
					}
					processed := false
					if item, processed = w.preprocess(item); processed {
						break
					}
					switch d := item.(type) {
					case error:
						_ = w.Broadcast(d)
						w.statManager.IncTotalExceptions(d.Error())
					case *xsql.Tuple:
						w.statManager.IncTotalRecordsIn()
						// Start the first event processing.
						// Later a series of events may send out in order
						w.statManager.ProcessTimeStart()
						// whether to drop the late event
						if w.track(ctx, d.Emitter, d.GetTimestamp()) {
							// If not drop, check if it can be sent out
							w.addAndTrigger(ctx, d)
						}
					default:
						e := fmt.Errorf("run watermark op error: expect *xsql.Tuple type but got %[1]T(%[1]v)", d)
						_ = w.Broadcast(e)
						w.statManager.IncTotalExceptions(e.Error())
					}
				}
			}
		})
		if err != nil {
			infra.DrainError(ctx, err, errCh)
		}
	}()
}

func (w *WatermarkOp) track(ctx api.StreamContext, emitter string, ts int64) bool {
	ctx.GetLogger().Debugf("watermark generator track event from topic %s at %d", emitter, ts)
	watermark, ok := w.streamWMs[emitter]
	if !ok || ts > watermark {
		w.streamWMs[emitter] = ts
		_ = ctx.PutState(StreamWMKey, w.streamWMs)
	}
	r := ts >= w.lastWatermarkTs
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
			return w.events[i].GetTimestamp() > d.GetTimestamp()
		})
		w.events = append(w.events, nil)
		copy(w.events[index+1:], w.events[index:])
		w.events[index] = d
	}

	watermark := w.computeWatermarkTs()
	ctx.GetLogger().Debugf("compute watermark event at %d with last %d", watermark, w.lastWatermarkTs)
	// Make sure watermark time proceeds
	if watermark > w.lastWatermarkTs {
		// Send out all events before the watermark
		if watermark >= w.events[0].GetTimestamp() {
			// Find out the last event to send in this watermark change
			c := len(w.events)
			for i, e := range w.events {
				if e.GetTimestamp() > watermark {
					c = i
					break
				}
			}

			// Send out all events before the watermark
			for i := 0; i < c; i++ {
				if i > 0 { // The first event processing time start at the beginning of event receiving
					w.statManager.ProcessTimeStart()
				}
				_ = w.Broadcast(w.events[i])
				ctx.GetLogger().Debug("send out event", w.events[i].GetTimestamp())
				w.statManager.IncTotalRecordsOut()
				w.statManager.ProcessTimeEnd()
			}
			w.events = w.events[c:]
			_ = ctx.PutState(EventInputKey, w.events)
		}
		// Update watermark
		if w.sendWatermark {
			_ = w.Broadcast(&xsql.WatermarkTuple{Timestamp: watermark, Tuple: d})
		}
		w.lastWatermarkTs = watermark
		_ = ctx.PutState(WatermarkKey, w.lastWatermarkTs)
		ctx.GetLogger().Debugf("scan watermark event at %d", watermark)
	}
}

// watermark is the minimum timestamp of all input topics
func (w *WatermarkOp) computeWatermarkTs() int64 {
	var ts int64 = math.MaxInt64
	for _, wm := range w.streamWMs {
		if ts > wm {
			ts = wm
		}
	}
	return ts - w.lateTolerance
}

func (w *WatermarkOp) GetMetrics() [][]interface{} {
	if w.statManager != nil {
		return [][]interface{}{
			w.statManager.GetMetrics(),
		}
	} else {
		return nil
	}
}
