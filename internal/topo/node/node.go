// Copyright 2021-2022 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo/checkpoint"
	"github.com/lf-edge/ekuiper/internal/topo/node/metric"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"strings"
)

type OperatorNode interface {
	api.Operator
	Broadcast(data interface{}) error
	GetStreamContext() api.StreamContext
	GetInputCount() int
	AddInputCount()
	SetQos(api.Qos)
	SetBarrierHandler(checkpoint.BarrierHandler)
}

type DataSourceNode interface {
	api.Emitter
	Open(ctx api.StreamContext, errCh chan<- error)
	GetName() string
	GetMetrics() [][]interface{}
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

func (o *defaultNode) Broadcast(val interface{}) error {
	switch d := val.(type) {
	case error:
		if !o.sendError {
			return nil
		}
	case xsql.TupleRow:
		val = d.Clone()
	case xsql.Collection:
		val = d.Clone()
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
			o.ctx.GetLogger().Errorf("drop message from %s to %s", o.name, name)
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

func printable(m map[string]interface{}) map[string]interface{} {
	printableMap := make(map[string]interface{})
	for k, v := range m {
		if strings.EqualFold(k, "password") {
			printableMap[k] = "*"
		} else {
			if vm, ok := v.(map[string]interface{}); ok {
				printableMap[k] = printable(vm)
			} else {
				printableMap[k] = v
			}
		}
	}
	return printableMap
}

func getSourceConf(ctx api.StreamContext, sourceType string, options *ast.Options) map[string]interface{} {
	confkey := options.CONF_KEY
	logger := ctx.GetLogger()
	confPath := "sources/" + sourceType + ".yaml"
	if sourceType == "mqtt" {
		confPath = "mqtt_source.yaml"
	}
	props := make(map[string]interface{})
	cfg := make(map[string]interface{})
	err := conf.LoadConfigByName(confPath, &cfg)
	if err != nil {
		logger.Warnf("fail to parse yaml for source %s. Return an empty configuration", sourceType)
	} else {
		def, ok := cfg["default"]
		if !ok {
			logger.Warnf("default conf is not found", confkey)
		} else {
			if def1, ok1 := def.(map[string]interface{}); ok1 {
				props = def1
			}
			if c, ok := cfg[strings.ToLower(confkey)]; ok {
				if c1, ok := c.(map[string]interface{}); ok {
					c2 := c1
					for k, v := range c2 {
						props[k] = v
					}
				}
			}
		}
	}
	f := options.FORMAT
	if f == "" {
		f = "json"
	}
	props["format"] = strings.ToLower(f)
	logger.Debugf("get conf for %s with conf key %s: %v", sourceType, confkey, printable(props))
	return props
}
