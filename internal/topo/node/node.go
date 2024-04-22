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
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/binder/io"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/util"
	"github.com/lf-edge/ekuiper/v2/internal/topo/checkpoint"
	"github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/topo/node/metric"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
)

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
	opsWg       *sync.WaitGroup
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

func (o *defaultNode) AddOutput(output chan<- any, name string) error {
	o.outputMu.Lock()
	defer o.outputMu.Unlock()
	o.outputs[name] = output
	return nil
}

func (o *defaultNode) RemoveOutput(name string) error {
	o.outputMu.Lock()
	defer o.outputMu.Unlock()
	namePre := name + "_"
	for n := range o.outputs {
		if strings.HasPrefix(n, namePre) {
			delete(o.outputs, n)
			if o.ctx != nil {
				o.ctx.GetLogger().Infof("Remove output %s from %s", n, o.name)
			}
		}
	}
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

func (o *defaultNode) Broadcast(val any) {
	o.BroadcastCustomized(val, o.doBroadcast)
}

func (o *defaultNode) BroadcastCustomized(val any, broadcastFunc func(val any)) {
	if _, ok := val.(error); ok && !o.sendError {
		return
	}
	if o.qos >= def.AtLeastOnce {
		boe := &checkpoint.BufferOrEvent{
			Data:    val,
			Channel: o.name,
		}
		broadcastFunc(boe)
		return
	}
	broadcastFunc(val)
	return
}

func (o *defaultNode) doBroadcast(val any) {
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
}

func newDefaultSinkNode(name string, options *def.RuleOption) *defaultSinkNode {
	return &defaultSinkNode{
		defaultNode: newDefaultNode(name, options),
		input:       make(chan any, options.BufferLength),
	}
}

func (o *defaultSinkNode) GetInput() (chan<- any, string) {
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

func (o *defaultNode) prepareExec(ctx api.StreamContext, errCh chan<- error, opType string) {
	ctx.GetLogger().Infof("%s started", o.name)
	o.statManager = metric.NewStatManager(ctx, opType)
	o.ctx = ctx
	wg := ctx.Value(context.RuleWaitGroupKey)
	if wg != nil {
		o.opsWg = wg.(*sync.WaitGroup)
	}
	if o.opsWg != nil {
		o.opsWg.Add(1)
	}
	o.ctrlCh = errCh
}

func (o *defaultNode) finishExec() {
	o.Close()
}

func (o *defaultNode) Close() {
	if o.opsWg != nil {
		o.opsWg.Done()
	}
}

func (o *defaultSinkNode) preprocess(ctx api.StreamContext, item any) (any, bool) {
	if o.qos >= def.AtLeastOnce {
		b, ok := item.(*checkpoint.BufferOrEvent)
		if ok {
			ctx.GetLogger().Debugf("data is BufferOrEvent, start barrier handler")
			// if it is a barrier, return true and ignore the further processing
			// if it is blocked(align handler), return true and then write back to the channel later
			if o.barrierHandler.Process(b, o.ctx) {
				return nil, true
			} else {
				return b.Data, false
			}
		}
	}
	return item, false
}

func (o *defaultSinkNode) commonIngest(ctx api.StreamContext, item any) (any, bool) {
	ctx.GetLogger().Debugf("op %s_%d receive %v", ctx.GetOpId(), ctx.GetInstanceId(), item)
	item, processed := o.preprocess(ctx, item)
	if processed {
		return item, processed
	}
	switch d := item.(type) {
	case error:
		if o.sendError {
			o.Broadcast(d)
		}
		return nil, true
	case *xsql.WatermarkTuple, xsql.EOFTuple:
		o.Broadcast(d)
		return nil, true
	}
	ctx.GetLogger().Debugf("%s_%d receive data %v", ctx.GetOpId(), ctx.GetInstanceId(), item)
	return item, false
}

func (o *defaultSinkNode) handleEof(ctx api.StreamContext, d xsql.EOFTuple) {
	if len(o.outputs) > 0 {
		o.Broadcast(d)
	} else {
		infra.DrainError(ctx, errors.New("done"), o.ctrlCh)
	}
}

func SourcePing(sourceType string, config map[string]any) error {
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

func LookupPing(lookupType string, config map[string]interface{}) error {
	lookup, err := io.LookupSource(lookupType)
	if err != nil {
		return err
	}
	dataSource := "/$$TEST_CONNECTION$$"
	if v, ok := config["DATASOURCE"]; ok {
		dataSource = v.(string)
	}
	if pingAble, ok := lookup.(util.PingableConn); ok {
		return pingAble.Ping(dataSource, config)
	}
	return fmt.Errorf("lookup %v doesn't support ping connection", lookup)
}
