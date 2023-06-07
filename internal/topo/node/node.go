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

	"github.com/lf-edge/ekuiper/internal/binder/io"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/checkpoint"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/topo/node/metric"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
)

type OperatorNode interface {
	api.Operator
	Broadcast(data interface{}) error
	GetStreamContext() api.StreamContext
	GetInputCount() int
	AddInputCount()
	SetQos(api.Qos)
	SetBarrierHandler(checkpoint.BarrierHandler)
	RemoveMetrics(name string)
}

type DataSourceNode interface {
	api.Emitter
	Open(ctx api.StreamContext, errCh chan<- error)
	GetName() string
	GetMetrics() [][]interface{}
	RemoveMetrics(ruleId string)
	Broadcast(val interface{}) error
	GetStreamContext() api.StreamContext
	SetQos(api.Qos)
}

type defaultNode struct {
	name         string
	outputs      map[string]chan<- interface{}
	concurrency  int
	sendError    bool
	statManagers []metric.StatManager
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

func (o *defaultNode) RemoveMetrics(ruleId string) {
	for _, stats := range o.statManagers {
		stats.Clean(ruleId)
	}
}

func (o *defaultNode) Broadcast(val interface{}) error {
	if _, ok := val.(error); ok && !o.sendError {
		return nil
	}
	if o.qos >= api.AtLeastOnce {
		boe := &checkpoint.BufferOrEvent{
			Data:    val,
			Channel: o.name,
		}
		o.doBroadcast(boe)
		return nil
	}
	o.doBroadcast(val)
	return nil
}

func (o *defaultNode) doBroadcast(val interface{}) {
	for name, out := range o.outputs {
		select {
		case out <- val:
			// do nothing
		case <-o.ctx.Done():
			// rule stop so stop waiting
		default:
			o.statManagers[0].IncTotalExceptions(fmt.Sprintf("buffer full, drop message from to %s", name))
			o.ctx.GetLogger().Debugf("drop message from %s to %s", o.name, name)
		}
		switch vt := val.(type) {
		case xsql.Collection:
			val = vt.Clone()
			break
		case xsql.TupleRow:
			val = vt.Clone()
		}
	}
}

func (o *defaultNode) GetStreamContext() api.StreamContext {
	return o.ctx
}

type defaultSinkNode struct {
	*defaultNode
	input          chan interface{}
	barrierHandler checkpoint.BarrierHandler
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

func (o *defaultSinkNode) SetBarrierHandler(bh checkpoint.BarrierHandler) {
	o.barrierHandler = bh
}

// return the data and if processed
func (o *defaultSinkNode) preprocess(data interface{}) (interface{}, bool) {
	if o.qos >= api.AtLeastOnce {
		logger := o.ctx.GetLogger()
		logger.Debugf("%s preprocess receive data %+v", o.name, data)
		b, ok := data.(*checkpoint.BufferOrEvent)
		if ok {
			logger.Debugf("data is BufferOrEvent, start barrier handler")
			// if it is a barrier, return true and ignore the further processing
			// if it is blocked(align handler), return true and then write back to the channel later
			if o.barrierHandler.Process(b, o.ctx) {
				return nil, true
			} else {
				return b.Data, false
			}
		}
	}
	// Filter all the watermark tuples.
	// Only event time window op needs this, so handle it there
	if _, ok := data.(*xsql.WatermarkTuple); ok {
		return nil, true
	}
	return data, false
}

func SinkOpen(sinkType string, config map[string]interface{}) error {
	sink, err := getSink(sinkType, config)
	if err != nil {
		return err
	}

	contextLogger := conf.Log.WithField("rule", "TestSinkOpen"+"_"+sinkType)
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)

	defer func() {
		_ = sink.Close(ctx)
	}()

	return sink.Open(ctx)
}

func SourceOpen(sourceType string, config map[string]interface{}) error {
	dataSource := "/$$TEST_CONNECTION$$"
	if v, ok := config["DATASOURCE"]; ok {
		dataSource = v.(string)
	}
	ns, err := io.Source(sourceType)
	if err != nil {
		return err
	}
	if ns == nil {
		lns, err := io.LookupSource(sourceType)
		if err != nil {
			return err
		}
		if lns == nil {
			// should not happen
			return fmt.Errorf("source %s not found", sourceType)
		}
		err = lns.Configure(dataSource, config)
		if err != nil {
			return err
		}

		contextLogger := conf.Log.WithField("rule", "TestSourceOpen"+"_"+sourceType)
		ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
		_ = lns.Close(ctx)
	} else {
		err = ns.Configure(dataSource, config)
		if err != nil {
			return err
		}

		contextLogger := conf.Log.WithField("rule", "TestSourceOpen"+"_"+sourceType)
		ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
		_ = ns.Close(ctx)
	}

	return nil
}
