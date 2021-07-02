package node

import (
	"context"
	"fmt"
	"github.com/emqx/kuiper/internal/xsql"
	"github.com/emqx/kuiper/pkg/api"
	"github.com/emqx/kuiper/pkg/ast"
	"math"
	"sort"
)

type WatermarkTuple struct {
	Timestamp int64
}

func (t *WatermarkTuple) GetTimestamp() int64 {
	return t.Timestamp
}

func (t *WatermarkTuple) IsWatermark() bool {
	return true
}

const WATERMARK_KEY = "$$wartermark"

type WatermarkGenerator struct {
	inputTopics   []string
	topicToTs     map[string]int64
	window        *WindowConfig
	lateTolerance int64
	interval      int
	//ticker          *clock.Ticker
	stream chan<- interface{}
	//state
	lastWatermarkTs int64
}

func NewWatermarkGenerator(window *WindowConfig, l int64, s []string, stream chan<- interface{}) (*WatermarkGenerator, error) {
	w := &WatermarkGenerator{
		window:        window,
		topicToTs:     make(map[string]int64),
		lateTolerance: l,
		inputTopics:   s,
		stream:        stream,
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
		//Use timeout to update watermark
		w.interval = window.Interval
	default:
		return nil, fmt.Errorf("unsupported window type %d", window.Type)
	}
	return w, nil
}

func (w *WatermarkGenerator) track(s string, ts int64, ctx api.StreamContext) bool {
	log := ctx.GetLogger()
	log.Debugf("watermark generator track event from topic %s at %d", s, ts)
	currentVal, ok := w.topicToTs[s]
	if !ok || ts > currentVal {
		w.topicToTs[s] = ts
	}
	r := ts >= w.lastWatermarkTs
	if r {
		w.trigger(ctx)
	}
	return r
}

func (w *WatermarkGenerator) trigger(ctx api.StreamContext) {
	log := ctx.GetLogger()
	watermark := w.computeWatermarkTs(ctx)
	log.Debugf("compute watermark event at %d with last %d", watermark, w.lastWatermarkTs)
	if watermark > w.lastWatermarkTs {
		t := &WatermarkTuple{Timestamp: watermark}
		select {
		case w.stream <- t:
		default: //TODO need to set buffer
		}
		w.lastWatermarkTs = watermark
		ctx.PutState(WATERMARK_KEY, w.lastWatermarkTs)
		log.Debugf("scan watermark event at %d", watermark)
	}
}

func (w *WatermarkGenerator) computeWatermarkTs(_ context.Context) int64 {
	var ts int64
	if len(w.topicToTs) >= len(w.inputTopics) {
		ts = math.MaxInt64
		for _, key := range w.inputTopics {
			if ts > w.topicToTs[key] {
				ts = w.topicToTs[key]
			}
		}
	}
	return ts - w.lateTolerance
}

//If window end cannot be determined yet, return max int64 so that it can be recalculated for the next watermark
func (w *WatermarkGenerator) getNextWindow(inputs []*xsql.Tuple, current int64, watermark int64, triggered bool) int64 {
	switch w.window.Type {
	case ast.TUMBLING_WINDOW, ast.HOPPING_WINDOW:
		if triggered {
			return current + int64(w.interval)
		} else {
			interval := int64(w.interval)
			nextTs := getEarliestEventTs(inputs, current, watermark)
			if nextTs == math.MaxInt64 || nextTs%interval == 0 {
				return nextTs
			}
			return nextTs + (interval - nextTs%interval)
		}
	case ast.SLIDING_WINDOW:
		nextTs := getEarliestEventTs(inputs, current, watermark)
		return nextTs
	default:
		return math.MaxInt64
	}
}

func (w *WatermarkGenerator) getNextSessionWindow(inputs []*xsql.Tuple, current int64, watermark int64, triggered bool) (int64, bool) {
	if len(inputs) > 0 {
		timeout, duration := int64(w.window.Interval), int64(w.window.Length)
		sort.SliceStable(inputs, func(i, j int) bool {
			return inputs[i].Timestamp < inputs[j].Timestamp
		})
		et := inputs[0].Timestamp
		tick := et + (duration - et%duration)
		if et%duration == 0 {
			tick = et
		}
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
	}
	return math.MaxInt64, false
}

func (o *WindowOperator) execEventWindow(ctx api.StreamContext, inputs []*xsql.Tuple, errCh chan<- error) {
	log := ctx.GetLogger()
	var (
		triggered       bool
		nextWindowEndTs int64
		prevWindowEndTs int64
		lastTicked      bool
	)

	o.watermarkGenerator.lastWatermarkTs = 0
	if s, err := ctx.GetState(WATERMARK_KEY); err == nil && s != nil {
		if si, ok := s.(int64); ok {
			o.watermarkGenerator.lastWatermarkTs = si
		} else {
			errCh <- fmt.Errorf("restore window state `lastWatermarkTs` %v error, invalid type", s)
		}
	}
	log.Infof("Start with window state lastWatermarkTs: %d", o.watermarkGenerator.lastWatermarkTs)
	for {
		select {
		// process incoming item
		case item, opened := <-o.input:
			processed := false
			if item, processed = o.preprocess(item); processed {
				break
			}
			o.statManager.ProcessTimeStart()
			if !opened {
				o.statManager.IncTotalExceptions()
				break
			}
			switch d := item.(type) {
			case error:
				o.statManager.IncTotalRecordsIn()
				o.Broadcast(d)
				o.statManager.IncTotalExceptions()
			case xsql.Event:
				if d.IsWatermark() {
					watermarkTs := d.GetTimestamp()
					windowEndTs := nextWindowEndTs
					ticked := false
					//Session window needs a recalculation of window because its window end depends the inputs
					if windowEndTs == math.MaxInt64 || o.window.Type == ast.SESSION_WINDOW || o.window.Type == ast.SLIDING_WINDOW {
						if o.window.Type == ast.SESSION_WINDOW {
							windowEndTs, ticked = o.watermarkGenerator.getNextSessionWindow(inputs, prevWindowEndTs, watermarkTs, triggered)
						} else {
							windowEndTs = o.watermarkGenerator.getNextWindow(inputs, prevWindowEndTs, watermarkTs, triggered)
						}
					}
					for windowEndTs <= watermarkTs && windowEndTs >= 0 {
						log.Debugf("Window end ts %d Watermark ts %d", windowEndTs, watermarkTs)
						log.Debugf("Current input count %d", len(inputs))
						//scan all events and find out the event in the current window
						if o.window.Type == ast.SESSION_WINDOW && !lastTicked {
							o.triggerTime = inputs[0].Timestamp
						}
						inputs, triggered = o.scan(inputs, windowEndTs, ctx)
						prevWindowEndTs = windowEndTs
						lastTicked = ticked
						if o.window.Type == ast.SESSION_WINDOW {
							windowEndTs, ticked = o.watermarkGenerator.getNextSessionWindow(inputs, prevWindowEndTs, watermarkTs, triggered)
						} else {
							windowEndTs = o.watermarkGenerator.getNextWindow(inputs, prevWindowEndTs, watermarkTs, triggered)
						}
					}
					nextWindowEndTs = windowEndTs
					log.Debugf("next window end %d", nextWindowEndTs)
				} else {
					o.statManager.IncTotalRecordsIn()
					tuple, ok := d.(*xsql.Tuple)
					if !ok {
						log.Debugf("receive non tuple element %v", d)
					}
					log.Debugf("event window receive tuple %s", tuple.Message)
					if o.watermarkGenerator.track(tuple.Emitter, d.GetTimestamp(), ctx) {
						inputs = append(inputs, tuple)
					}
				}
				o.statManager.ProcessTimeEnd()
				ctx.PutState(WINDOW_INPUTS_KEY, inputs)
			default:
				o.statManager.IncTotalRecordsIn()
				o.Broadcast(fmt.Errorf("run Window error: expect xsql.Event type but got %[1]T(%[1]v)", d))
				o.statManager.IncTotalExceptions()
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
