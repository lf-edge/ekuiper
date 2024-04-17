// Copyright 2021-2024 EMQ Technologies Co., Ltd.
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
	"sync"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/binder/io"
	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/util"
	"github.com/lf-edge/ekuiper/v2/internal/topo/checkpoint"
	"github.com/lf-edge/ekuiper/v2/internal/topo/node/metric"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
)

type OperatorNode interface {
	api.Operator
	Broadcast(data interface{})
	GetStreamContext() api.StreamContext
	GetInputCount() int
	AddInputCount()
	SetQos(def.Qos)
	SetBarrierHandler(checkpoint.BarrierHandler)
	RemoveMetrics(name string)
}

type SchemaNode interface {
	// AttachSchema attach the schema to the node. The parameters are ruleId, sourceName, schema, whether is wildcard
	AttachSchema(api.StreamContext, string, map[string]*ast.JsonStreamField, bool)
	// DetachSchema detach the schema from the node. The parameters are ruleId
	DetachSchema(string)
}

type DataSourceNode interface {
	api.Emitter
	Open(ctx api.StreamContext, errCh chan<- error)
	GetName() string
	GetMetrics() []any
	RemoveMetrics(ruleId string)
}

type SourceInstanceNode interface {
	GetSource() api.Source
}

type MergeableTopo interface {
	GetSource() DataSourceNode
	// MergeSrc Add child topo as the source with following operators
	MergeSrc(parentTopo *def.PrintableTopo)
	// LinkTopo Add printable topo link from the parent topo to the child topo
	LinkTopo(parentTopo *def.PrintableTopo, parentJointName string)
	// SubMetrics return the metrics of the sub nodes
	SubMetrics() ([]string, []any)
	// Close notifies subtopo to deref
	Close(ruleId string)
}

type defaultNode struct {
	name        string
	concurrency int
	sendError   bool
	statManager metric.StatManager
	ctx         api.StreamContext
	ctrlCh      chan<- error
	qos         def.Qos
	outputMu    sync.RWMutex
	outputs     map[string]chan<- any
}

func newDefaultNode(name string, options *def.RuleOption) *defaultNode {
	c := options.Concurrency
	if c < 1 {
		c = 1
	}
	return &defaultNode{
		name:        name,
		outputs:     make(map[string]chan<- any),
		concurrency: c,
		sendError:   options.SendError,
	}
}

func (o *defaultNode) AddOutput(output chan<- interface{}, name string) error {
	o.outputMu.Lock()
	defer o.outputMu.Unlock()
	o.outputs[name] = output
	return nil
}

func (o *defaultNode) GetName() string {
	return o.name
}

func (o *defaultNode) SetQos(qos def.Qos) {
	o.qos = qos
}

func (o *defaultNode) GetMetrics() []any {
	if o.statManager != nil {
		return o.statManager.GetMetrics()
	}
	return nil
}

func (o *defaultNode) RemoveMetrics(ruleId string) {
	if o.statManager != nil {
		o.statManager.Clean(ruleId)
	}
}

func (o *defaultNode) Broadcast(val interface{}) {
	if _, ok := val.(error); ok && !o.sendError {
		return
	}
	if o.qos >= def.AtLeastOnce {
		boe := &checkpoint.BufferOrEvent{
			Data:    val,
			Channel: o.name,
		}
		o.doBroadcast(boe)
		return
	}
	o.doBroadcast(val)
	return
}

func (o *defaultNode) doBroadcast(val interface{}) {
	o.outputMu.RLock()
	defer o.outputMu.RUnlock()
	l := len(o.outputs)
	c := 0
	for name, out := range o.outputs {
		select {
		case out <- val:
			// do nothing
		case <-o.ctx.Done():
			// rule stop so stop waiting
		default:
			o.statManager.IncTotalExceptions(fmt.Sprintf("buffer full, drop message from %s to %s", o.name, name))
			o.ctx.GetLogger().Debugf("drop message from %s to %s", o.name, name)
		}
		c++
		if c == l {
			break
		}
		switch vt := val.(type) {
		case xsql.Collection:
			val = vt.Clone()
			break
		case xsql.Row:
			val = vt.Clone()
		}
	}
}

func (o *defaultNode) GetStreamContext() api.StreamContext {
	return o.ctx
}

type defaultSinkNode struct {
	*defaultNode
	input          chan any
	barrierHandler checkpoint.BarrierHandler
	inputCount     int
	bufferLen      int
}

func newDefaultSinkNode(name string, options *def.RuleOption) *defaultSinkNode {
	return &defaultSinkNode{
		bufferLen:   options.BufferLength,
		defaultNode: newDefaultNode(name, options),
		input:       make(chan any, options.BufferLength),
	}
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
	if o.qos >= def.AtLeastOnce {
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
	return data, false
}

func (o *defaultNode) prepareExec(ctx api.StreamContext, errCh chan<- error, opType string) {
	ctx.GetLogger().Infof("%s started", o.name)
	o.statManager = metric.NewStatManager(ctx, opType)
	o.ctx = ctx
	o.ctrlCh = errCh
}

func SourcePing(sourceType string, config map[string]interface{}) error {
	source, err := io.Source(sourceType)
	if err != nil {
		return err
	}
	dataSource := "/$$TEST_CONNECTION$$"
	if v, ok := config["DATASOURCE"]; ok {
		dataSource = v.(string)
	}
	if pingAble, ok := source.(util.PingableConn); ok {
		return pingAble.Ping(dataSource, config)
	}
	return fmt.Errorf("source %v doesn't support ping connection", sourceType)
}

func SinkPing(sinkType string, config map[string]interface{}) error {
	sink, err := getSink(sinkType, config)
	if err != nil {
		return err
	}
	if pingAble, ok := sink.(util.PingableConn); ok {
		return pingAble.Ping("", config)
	}
	return fmt.Errorf("sink %v doesn't support ping connection", sinkType)
}

func propsToNodeOption(props map[string]any) *def.RuleOption {
	options := &def.RuleOption{
		BufferLength: 1024,
		SendError:    true,
		Qos:          def.AtLeastOnce,
	}
	err := cast.MapToStruct(props, options)
	if err != nil {
		conf.Log.Warnf("fail to parse rule option %v from props", err)
	}
	return options
}
