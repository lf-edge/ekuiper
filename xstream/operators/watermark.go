package operators

import (
	"context"
	"engine/common"
	"engine/xsql"
	"engine/xstream/api"
	"fmt"
	"math"
	"sort"
	"time"
)

type WatermarkTuple struct{
	Timestamp int64
}

func (t *WatermarkTuple) GetTimestamp() int64 {
	return t.Timestamp
}

func (t *WatermarkTuple) IsWatermark() bool {
	return true
}

type WatermarkGenerator struct {
	lastWatermarkTs int64
	inputTopics []string
	topicToTs map[string]int64
	window      *WindowConfig
	lateTolerance int64
	interval	int
	ticker 		common.Ticker
	stream 	chan<- interface{}
}

func NewWatermarkGenerator(window *WindowConfig, l int64, s []string, stream chan<- interface{}) (*WatermarkGenerator, error){
	w := &WatermarkGenerator{
		window: window,
		topicToTs: make(map[string]int64),
		lateTolerance: l,
		inputTopics: s,
		stream: stream,
	}
	//Tickers to update watermark
	switch window.Type{
	case xsql.NOT_WINDOW:
	case xsql.TUMBLING_WINDOW:
		w.ticker = common.GetTicker(window.Length)
		w.interval = window.Length
	case xsql.HOPPING_WINDOW:
		w.ticker = common.GetTicker(window.Interval)
		w.interval = window.Interval
	case xsql.SLIDING_WINDOW:
		w.interval = window.Length
	case xsql.SESSION_WINDOW:
		//Use timeout to update watermark
		w.ticker = common.GetTicker(window.Interval)
		w.interval = window.Interval
	default:
		return nil, fmt.Errorf("unsupported window type %d", window.Type)
	}
	return w, nil
}

func (w *WatermarkGenerator) track(s string, ts int64, ctx api.StreamContext) bool {
	log := ctx.GetLogger()
	log.Infof("watermark generator track event from topic %s at %d", s, ts)
	currentVal, ok := w.topicToTs[s]
	if !ok || ts > currentVal {
		w.topicToTs[s] = ts
	}
	r := ts >= w.lastWatermarkTs
	if r{
		switch w.window.Type{
		case xsql.SLIDING_WINDOW:
			w.trigger(ctx)
		}
	}
	return r
}

func (w *WatermarkGenerator) start(ctx api.StreamContext) {
	log := ctx.GetLogger()
	var c <-chan time.Time

	if w.ticker != nil {
		c = w.ticker.GetC()
	}
	for {
		select {
		case <-c:
			w.trigger(ctx)
		case <-ctx.Done():
			log.Infoln("Cancelling watermark generator....")
			if w.ticker != nil{
				w.ticker.Stop()
			}
			return
		}
	}
}

func (w *WatermarkGenerator) trigger(ctx api.StreamContext) {
	log := ctx.GetLogger()
	watermark := w.computeWatermarkTs(ctx)
	log.Infof("compute watermark event at %d with last %d", watermark, w.lastWatermarkTs)
	if watermark > w.lastWatermarkTs {
		t := &WatermarkTuple{Timestamp: watermark}
		select {
		case w.stream <- t:
		default: //TODO need to set buffer
		}
		w.lastWatermarkTs = watermark
		log.Infof("scan watermark event at %d", watermark)
	}
}

func (w *WatermarkGenerator) computeWatermarkTs(ctx context.Context) int64{
	var ts int64
	if len(w.topicToTs) >= len(w.inputTopics) {
		ts = math.MaxInt64
		for _, key := range w.inputTopics {
			if ts > w.topicToTs[key]{
				ts = w.topicToTs[key]
			}
		}
	}
	return ts - w.lateTolerance
}

//If window end cannot be determined yet, return max int64 so that it can be recalculated for the next watermark
func (w *WatermarkGenerator) getNextWindow(inputs []*xsql.Tuple,current int64, watermark int64, triggered bool) int64{
	switch w.window.Type{
	case xsql.TUMBLING_WINDOW, xsql.HOPPING_WINDOW:
		if triggered{
			return current + int64(w.interval)
		}else{
			interval := int64(w.interval)
			nextTs := getEarliestEventTs(inputs, current, watermark)
			if nextTs == math.MaxInt64 ||  nextTs % interval == 0{
				return nextTs
			}
			return nextTs + (interval - nextTs % interval)
		}
	case xsql.SLIDING_WINDOW:
		nextTs := getEarliestEventTs(inputs, current, watermark)
		return nextTs
	case xsql.SESSION_WINDOW:
		if len(inputs) > 0{
			timeout, duration := int64(w.window.Interval), int64(w.window.Length)
			sort.SliceStable(inputs, func(i, j int) bool {
				return inputs[i].Timestamp < inputs[j].Timestamp
			})
			et := inputs[0].Timestamp
			tick := et + (duration - et % duration)
			if et % duration == 0 {
				tick = et
			}
			var p int64
			for _, tuple := range inputs {
				var r int64 = math.MaxInt64
				if p > 0{
					if tuple.Timestamp - p > timeout{
						r = p + timeout
					}
				}
				if tuple.Timestamp > tick {
					if tick - duration > et && tick < r {
						r = tick
					}
					tick += duration
				}
				if r < math.MaxInt64{
					return r
				}
				p = tuple.Timestamp
			}
		}
		return math.MaxInt64
	default:
		return math.MaxInt64
	}
}

func (o *WindowOperator) execEventWindow(ctx api.StreamContext, errCh chan<- error) {
	exeCtx, cancel := ctx.WithCancel()
	log := ctx.GetLogger()
	go o.watermarkGenerator.start(exeCtx)
	var (
		inputs []*xsql.Tuple
		triggered bool
		nextWindowEndTs int64
		prevWindowEndTs int64
	)

	for {
		select {
		// process incoming item
		case item, opened := <-o.input:
			if !opened {
				break
			}
			if d, ok := item.(xsql.Event); !ok {
				log.Errorf("Expect xsql.Event type")
				break
			}else{
				if d.IsWatermark(){
					watermarkTs := d.GetTimestamp()
					windowEndTs := nextWindowEndTs
					//Session window needs a recalculation of window because its window end depends the inputs
					if windowEndTs == math.MaxInt64 || o.window.Type == xsql.SESSION_WINDOW || o.window.Type == xsql.SLIDING_WINDOW{
						windowEndTs = o.watermarkGenerator.getNextWindow(inputs, prevWindowEndTs, watermarkTs, triggered)
					}
					for windowEndTs <= watermarkTs && windowEndTs >= 0 {
						log.Debugf("Window end ts %d Watermark ts %d", windowEndTs, watermarkTs)
						log.Debugf("Current input count %d", len(inputs))
						//scan all events and find out the event in the current window
						inputs, triggered = o.scan(inputs, windowEndTs, ctx)
						prevWindowEndTs = windowEndTs
						windowEndTs = o.watermarkGenerator.getNextWindow(inputs, windowEndTs, watermarkTs, triggered)
					}
					nextWindowEndTs = windowEndTs
					log.Debugf("next window end %d", nextWindowEndTs)
				}else{
					tuple, ok := d.(*xsql.Tuple)
					if !ok{
						log.Infof("receive non tuple element %v", d)
					}
					log.Debugf("event window receive tuple %s", tuple.Message)
					if o.watermarkGenerator.track(tuple.Emitter, d.GetTimestamp(),ctx){
						inputs = append(inputs, tuple)
					}
				}

			}
		// is cancelling
		case <-ctx.Done():
			log.Infoln("Cancelling window....")
			if o.ticker != nil{
				o.ticker.Stop()
			}
			cancel()
			return
		}
	}
}

func getEarliestEventTs(inputs []*xsql.Tuple, startTs int64, endTs int64) int64{
	var minTs int64 = math.MaxInt64
	for _, t := range inputs{
		if t.Timestamp > startTs && t.Timestamp <= endTs && t.Timestamp < minTs{
			minTs = t.Timestamp
		}
	}
	return minTs
}