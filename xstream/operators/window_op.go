package operators

import (
	"context"
	"engine/common"
	"engine/xsql"
	"fmt"
	"math"
	"time"
)

type WindowType int
const (
	NO_WINDOW WindowType = iota
	TUMBLING_WINDOW
	HOPPING_WINDOW
	SLIDING_WINDOW
	SESSION_WINDOW
)

type WindowConfig struct {
	Type WindowType
	Length int64
	Interval int64   //If interval is not set, it is equals to Length
}

type WindowOperator struct {
	input       chan interface{}
	outputs     map[string]chan<- interface{}
	name 		string
	ticker 		*time.Ticker
	window      *WindowConfig
	interval	int64
	triggerTime int64
}

func NewWindowOp(name string, config *WindowConfig) *WindowOperator {
	o := new(WindowOperator)

	o.input = make(chan interface{}, 1024)
	o.outputs = make(map[string]chan<- interface{})
	o.name = name
	o.window = config
	switch config.Type{
	case NO_WINDOW:
	case TUMBLING_WINDOW:
		o.ticker = time.NewTicker(time.Duration(config.Length) * time.Millisecond)
		o.interval = config.Length
	case HOPPING_WINDOW:
		o.ticker = time.NewTicker(time.Duration(config.Interval) * time.Millisecond)
		o.interval = config.Interval
	case SLIDING_WINDOW:
		o.interval = config.Length
	case SESSION_WINDOW:
		o.interval = config.Interval
	default:
		log.Errorf("Unsupported window type %d", config.Type)
	}

	return o
}

func (o *WindowOperator) GetName() string {
	return o.name
}

func (o *WindowOperator) AddOutput(output chan<- interface{}, name string) {
	if _, ok := o.outputs[name]; !ok{
		o.outputs[name] = output
	}else{
		log.Error("fail to add output %s, operator %s already has an output of the same name", name, o.name)
	}
}

func (o *WindowOperator) GetInput() (chan<- interface{}, string) {
	return o.input, o.name
}

// Exec is the entry point for the executor
func (o *WindowOperator) Exec(ctx context.Context) (err error) {

	log.Printf("Window operator %s is started.\n", o.name)

	if len(o.outputs) <= 0 {
		err = fmt.Errorf("no output channel found")
		return
	}

	go func() {
		var (
			inputs []*xsql.Tuple
			c <-chan time.Time
			timeoutTicker *time.Timer
			timeout <-chan time.Time
		)

		if o.ticker != nil {
			c = o.ticker.C
		}

		for {
			select {
			// process incoming item
			case item, opened := <-o.input:
				if !opened {
					return
				}
				if d, ok := item.(*xsql.Tuple); !ok {
					log.Errorf("Expect xsql.Tuple type.\n")
					return
				}else{
					inputs = append(inputs, d)
					switch o.window.Type{
					case NO_WINDOW:
						inputs = o.trigger(inputs, d.Timestamp)
					case SLIDING_WINDOW:
						inputs = o.trigger(inputs, d.Timestamp)
					case SESSION_WINDOW:
						if o.ticker == nil{ //Stopped by timeout or init
							o.ticker = time.NewTicker(time.Duration(o.window.Length) * time.Millisecond)
							c = o.ticker.C
						}
						if timeoutTicker != nil {
							timeoutTicker.Stop()
							timeoutTicker.Reset(time.Duration(o.window.Interval) * time.Millisecond)
						} else {
							timeoutTicker = time.NewTimer(time.Duration(o.window.Interval) * time.Millisecond)
							timeout = timeoutTicker.C
						}
					}
				}
			case now := <-c:
				if len(inputs) > 0 {
					log.Infof("triggered by ticker")
					inputs = o.trigger(inputs, common.TimeToUnixMilli(now))
				}
			case now := <-timeout:
				if len(inputs) > 0 {
					log.Infof("triggered by timeout")
					inputs = o.trigger(inputs, common.TimeToUnixMilli(now))
				}
				o.ticker.Stop()
				o.ticker = nil
			// is cancelling
			case <-ctx.Done():
				log.Println("Cancelling....")
				o.ticker.Stop()
				return
			}
		}
	}()

	return nil
}

func (o *WindowOperator) trigger(inputs []*xsql.Tuple, triggerTime int64) []*xsql.Tuple{
	log.Printf("window %s triggered at %s", o.name, triggerTime)
	var delta int64
	if o.window.Type == HOPPING_WINDOW || o.window.Type == SLIDING_WINDOW {
		lastTriggerTime := o.triggerTime
		o.triggerTime = triggerTime
		if lastTriggerTime <= 0 {
			delta = math.MaxInt32  //max int, all events for the initial window
		}else{
			delta = o.triggerTime - lastTriggerTime - o.window.Interval
			if delta > 100 && o.window.Interval > 0 {
				log.Warnf("Possible long computation in window; Previous eviction time: %d, current eviction time: %d", lastTriggerTime, o.triggerTime)
			}
		}
	}
	var results xsql.MultiEmitterTuples = make([]xsql.EmitterTuples, 0)
	i := 0
	//Sync table
	for _, tuple := range inputs{
		if o.window.Type == HOPPING_WINDOW || o.window.Type == SLIDING_WINDOW {
			diff := o.triggerTime - tuple.Timestamp
			if diff >= o.window.Length + delta {
				log.Infof("diff: %d, length: %d, delta: %d", diff, o.window.Length, delta)
				log.Infof("tuple %s emitted at %d expired", tuple, tuple.Timestamp)
				//Expired tuple, remove it by not adding back to inputs
				continue
			}
			//All tuples in tumbling window are not added back
			inputs[i] = tuple
			i++
		}
		results.AddTuple(tuple)
	}
	if len(results) > 0{
		log.Printf("window %s triggered for %d tuples", o.name, len(results))
		for _, output := range o.outputs{
			output <- results
		}
	}

	return inputs[:i]
}