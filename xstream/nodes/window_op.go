package nodes

import (
	"fmt"
	"github.com/benbjohnson/clock"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/api"
	"math"
	"time"
)

type WindowConfig struct {
	Type     xsql.WindowType
	Length   int
	Interval int //If interval is not set, it is equals to Length
}

type WindowOperator struct {
	*defaultSinkNode
	ticker             *clock.Ticker //For processing time only
	window             *WindowConfig
	interval           int
	triggerTime        int64
	isEventTime        bool
	statManager        StatManager
	watermarkGenerator *WatermarkGenerator //For event time only
	msgCount           int
}

func NewWindowOp(name string, w *xsql.Window, isEventTime bool, lateTolerance int64, streams []string, bufferLength int) (*WindowOperator, error) {
	o := new(WindowOperator)

	o.defaultSinkNode = &defaultSinkNode{
		input: make(chan interface{}, bufferLength),
		defaultNode: &defaultNode{
			outputs: make(map[string]chan<- interface{}),
			name:    name,
		},
	}
	o.isEventTime = isEventTime
	if w != nil {
		o.window = &WindowConfig{
			Type:   w.WindowType,
			Length: w.Length.Val,
		}
		if w.Interval != nil {
			o.window.Interval = w.Interval.Val
		} else if o.window.Type == xsql.COUNT_WINDOW {
			//if no interval value is set and it's count window, then set interval to length value.
			o.window.Interval = o.window.Length
		}
	} else {
		o.window = &WindowConfig{
			Type: xsql.NOT_WINDOW,
		}
	}

	if isEventTime {
		//Create watermark generator
		if w, err := NewWatermarkGenerator(o.window, lateTolerance, streams, o.input); err != nil {
			return nil, err
		} else {
			o.watermarkGenerator = w
		}
	} else {
		switch o.window.Type {
		case xsql.NOT_WINDOW:
		case xsql.TUMBLING_WINDOW:
			o.ticker = common.GetTicker(o.window.Length)
			o.interval = o.window.Length
		case xsql.HOPPING_WINDOW:
			o.ticker = common.GetTicker(o.window.Interval)
			o.interval = o.window.Interval
		case xsql.SLIDING_WINDOW:
			o.interval = o.window.Length
		case xsql.SESSION_WINDOW:
			o.ticker = common.GetTicker(o.window.Length)
			o.interval = o.window.Interval
		case xsql.COUNT_WINDOW:
			o.interval = o.window.Interval
		default:
			return nil, fmt.Errorf("unsupported window type %d", o.window.Type)
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
		go func() { errCh <- fmt.Errorf("no output channel found") }()
		return
	}
	stats, err := NewStatManager("op", ctx)
	if err != nil {
		go func() { errCh <- err }()
		return
	}
	o.statManager = stats
	if o.isEventTime {
		go o.execEventWindow(ctx, errCh)
	} else {
		go o.execProcessingWindow(ctx, errCh)
	}
}

func (o *WindowOperator) execProcessingWindow(ctx api.StreamContext, errCh chan<- error) {
	log := ctx.GetLogger()
	var (
		inputs        []*xsql.Tuple
		c             <-chan time.Time
		timeoutTicker *clock.Timer
		timeout       <-chan time.Time
	)

	if o.ticker != nil {
		c = o.ticker.C
	}

	for {
		select {
		// process incoming item
		case item, opened := <-o.input:
			o.statManager.IncTotalRecordsIn()
			o.statManager.ProcessTimeStart()
			if !opened {
				o.statManager.IncTotalExceptions()
				break
			}
			switch d := item.(type) {
			case error:
				o.Broadcast(d)
				o.statManager.IncTotalExceptions()
			case *xsql.Tuple:
				log.Debugf("Event window receive tuple %s", d.Message)
				inputs = append(inputs, d)
				switch o.window.Type {
				case xsql.NOT_WINDOW:
					inputs, _ = o.scan(inputs, d.Timestamp, ctx)
				case xsql.SLIDING_WINDOW:
					inputs, _ = o.scan(inputs, d.Timestamp, ctx)
				case xsql.SESSION_WINDOW:
					if timeoutTicker != nil {
						timeoutTicker.Stop()
						timeoutTicker.Reset(time.Duration(o.window.Interval) * time.Millisecond)
					} else {
						timeoutTicker = common.GetTimer(o.window.Interval)
						timeout = timeoutTicker.C
					}
				case xsql.COUNT_WINDOW:
					o.msgCount++
					log.Debugf(fmt.Sprintf("msgCount: %d", o.msgCount))
					if o.msgCount%o.window.Interval != 0 {
						continue
					} else {
						o.msgCount = 0
					}

					if tl, er := NewTupleList(inputs, o.window.Length); er != nil {
						log.Error(fmt.Sprintf("Found error when trying to "))
						errCh <- er
					} else {
						log.Debugf(fmt.Sprintf("It has %d of count window.", tl.count()))
						for tl.hasMoreCountWindow() {
							tsets := tl.nextCountWindow()
							log.Debugf("Sent: %v", tsets)
							//blocking if one of the channel is full
							o.Broadcast(tsets)
							o.statManager.IncTotalRecordsOut()
						}
						inputs = tl.getRestTuples()
					}
				}
				o.statManager.ProcessTimeEnd()
				o.statManager.SetBufferLength(int64(len(o.input)))
			default:
				o.Broadcast(fmt.Errorf("run Window error: expect xsql.Tuple type but got %[1]T(%[1]v)", d))
				o.statManager.IncTotalExceptions()
			}
		case now := <-c:
			if len(inputs) > 0 {
				o.statManager.ProcessTimeStart()
				n := common.TimeToUnixMilli(now)
				//For session window, check if the last scan time is newer than the inputs
				if o.window.Type == xsql.SESSION_WINDOW {
					//scan time for session window will record all triggers of the ticker but not the timeout
					lastTriggerTime := o.triggerTime
					o.triggerTime = n
					//Check if the current window has exceeded the max duration, if not continue expand
					if lastTriggerTime < inputs[0].Timestamp {
						break
					}
				}
				log.Debugf("triggered by ticker")
				inputs, _ = o.scan(inputs, n, ctx)
				o.statManager.ProcessTimeEnd()
			}
		case now := <-timeout:
			if len(inputs) > 0 {
				o.statManager.ProcessTimeStart()
				log.Debugf("triggered by timeout")
				inputs, _ = o.scan(inputs, common.TimeToUnixMilli(now), ctx)
				//expire all inputs, so that when timer scan there is no item
				inputs = make([]*xsql.Tuple, 0)
				o.statManager.ProcessTimeEnd()
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

type TupleList struct {
	tuples []*xsql.Tuple
	index  int //Current index
	size   int //The size for count window
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

func (tl *TupleList) nextCountWindow() xsql.WindowTuplesSet {
	var results xsql.WindowTuplesSet = make([]xsql.WindowTuples, 0)
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

func (o *WindowOperator) scan(inputs []*xsql.Tuple, triggerTime int64, ctx api.StreamContext) ([]*xsql.Tuple, bool) {
	log := ctx.GetLogger()
	log.Debugf("window %s triggered at %s(%d)", o.name, time.Unix(triggerTime/1000, triggerTime%1000), triggerTime)
	var delta int64
	if o.window.Type == xsql.HOPPING_WINDOW || o.window.Type == xsql.SLIDING_WINDOW {
		delta = o.calDelta(triggerTime, delta, log)
	}
	var results xsql.WindowTuplesSet = make([]xsql.WindowTuples, 0)
	i := 0
	//Sync table
	for _, tuple := range inputs {
		if o.window.Type == xsql.HOPPING_WINDOW || o.window.Type == xsql.SLIDING_WINDOW {
			diff := o.triggerTime - tuple.Timestamp
			if diff > int64(o.window.Length)+delta {
				log.Debugf("diff: %d, length: %d, delta: %d", diff, o.window.Length, delta)
				log.Debugf("tuple %s emitted at %d expired", tuple, tuple.Timestamp)
				//Expired tuple, remove it by not adding back to inputs
				continue
			}
			//Added back all inputs for non expired events
			inputs[i] = tuple
			i++
		} else if tuple.Timestamp > triggerTime {
			//Only added back early arrived events
			inputs[i] = tuple
			i++
		}
		if tuple.Timestamp <= triggerTime {
			results = results.AddTuple(tuple)
		}
	}
	triggered := false
	if len(results) > 0 {
		log.Debugf("window %s triggered for %d tuples", o.name, len(inputs))
		if o.isEventTime {
			results.Sort()
		}
		log.Debugf("Sent: %v", results)
		//blocking if one of the channel is full
		o.Broadcast(results)
		triggered = true
		o.statManager.IncTotalRecordsOut()
		log.Debugf("done scan")
	}

	return inputs[:i], triggered
}

func (o *WindowOperator) calDelta(triggerTime int64, delta int64, log api.Logger) int64 {
	lastTriggerTime := o.triggerTime
	o.triggerTime = triggerTime
	if lastTriggerTime <= 0 {
		delta = math.MaxInt16 //max int, all events for the initial window
	} else {
		if !o.isEventTime && o.window.Interval > 0 {
			delta = o.triggerTime - lastTriggerTime - int64(o.window.Interval)
			if delta > 100 {
				log.Warnf("Possible long computation in window; Previous eviction time: %d, current eviction time: %d", lastTriggerTime, o.triggerTime)
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
