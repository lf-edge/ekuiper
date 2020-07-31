package nodes

import (
	"fmt"
	"github.com/emqx/kuiper/xstream/api"
	"github.com/emqx/kuiper/xstream/checkpoints"
	"sync"
)

type OperatorNode interface {
	api.Operator
	Broadcast(data interface{}) error
	GetStreamContext() api.StreamContext
	GetInputCount() int
	AddInputCount()
	SetQos(api.Qos)
	SetBarrierHandler(checkpoints.BarrierHandler)
}

type defaultNode struct {
	name         string
	outputs      map[string]chan<- interface{}
	concurrency  int
	statManagers []StatManager
	ctx          api.StreamContext
	qos          api.Qos
}

func (o *defaultNode) AddOutput(output chan<- interface{}, name string) error {
	if _, ok := o.outputs[name]; !ok {
		o.outputs[name] = output
	} else {
		return fmt.Errorf("fail to add output %s, node %s already has an output of the same name", name, o.name)
	}
	return nil
}

func (o *defaultNode) GetName() string {
	return o.name
}

// SetConcurrency sets the concurrency level for the operation
func (o *defaultNode) SetConcurrency(concurr int) {
	o.concurrency = concurr
	if o.concurrency < 1 {
		o.concurrency = 1
	}
}

func (o *defaultNode) SetQos(qos api.Qos) {
	o.qos = qos
}

func (o *defaultNode) GetMetrics() (result [][]interface{}) {
	for _, stats := range o.statManagers {
		result = append(result, stats.GetMetrics())
	}
	return result
}

func (o *defaultNode) Broadcast(val interface{}) error {
	if o.qos >= api.AtLeastOnce {
		boe := &checkpoints.BufferOrEvent{
			Data:    val,
			Channel: o.name,
		}
		return o.doBroadcast(boe)
	}
	return o.doBroadcast(val)
}

func (o *defaultNode) doBroadcast(val interface{}) error {
	logger := o.ctx.GetLogger()
	var wg sync.WaitGroup
	wg.Add(len(o.outputs))
	for n, out := range o.outputs {
		go func(name string, output chan<- interface{}) {
			output <- val
			wg.Done()
			logger.Debugf("broadcast from %s to %s done", o.ctx.GetOpId(), name)
		}(n, out)
	}
	logger.Debugf("broadcasting from %s", o.ctx.GetOpId())
	wg.Wait()
	return nil
}

func (o *defaultNode) GetStreamContext() api.StreamContext {
	return o.ctx
}

type defaultSinkNode struct {
	*defaultNode
	input          chan interface{}
	barrierHandler checkpoints.BarrierHandler
	inputCount     int
}

func (o *defaultSinkNode) GetInput() (chan<- interface{}, string) {
	return o.input, o.name
}

func (o *defaultSinkNode) GetInputCount() int {
	return o.inputCount
}

func (o *defaultSinkNode) AddInputCount() {
	o.inputCount++
}

func (o *defaultSinkNode) SetBarrierHandler(bh checkpoints.BarrierHandler) {
	o.barrierHandler = bh
}

// return the data and if processed
func (o *defaultSinkNode) preprocess(data interface{}) (interface{}, bool) {
	if o.qos >= api.AtLeastOnce {
		logger := o.ctx.GetLogger()
		logger.Debugf("%s preprocess receive data %+v", o.name, data)
		b, ok := data.(*checkpoints.BufferOrEvent)
		if ok {
			logger.Debugf("data is BufferOrEvent, start barrier handler")
			//if it is barrier return true and ignore the further processing
			//if it is blocked(align handler), return true and then write back to the channel later
			if o.barrierHandler.Process(b, o.ctx) {
				return nil, true
			} else {
				return b.Data, false
			}
		}
	}
	return data, false
}
