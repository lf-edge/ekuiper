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
	"strings"
	"sync"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/lf-edge/ekuiper/v2/internal/binder/io"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/util"
	"github.com/lf-edge/ekuiper/v2/internal/topo/checkpoint"
	"github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/topo/node/metric"
	"github.com/lf-edge/ekuiper/v2/internal/topo/node/tracenode"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
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
	outputs     map[string]chan any
	opsWg       *sync.WaitGroup
	// tracing state
	span                     trace.Span
	spanCtx                  api.StreamContext
	disableBufferFullDiscard bool
	isStatManagerHostBySink  bool
}

func newDefaultNode(name string, options *def.RuleOption) *defaultNode {
	c := options.Concurrency
	if c < 1 {
		c = 1
	}
	return &defaultNode{
		name:                     name,
		outputs:                  make(map[string]chan any),
		concurrency:              c,
		sendError:                options.SendError,
		disableBufferFullDiscard: options.DisableBufferFullDiscard,
	}
}

func (o *defaultNode) AddOutput(output chan any, name string) error {
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
	last := len(o.outputs) - 1
	i := 0
	var valCopy any
	for name, out := range o.outputs {
		// Only copy tuples except the last one(copy previous one may change val, so copy the last) when there are many outputs to save one copy time
		if i != last {
			switch vt := val.(type) {
			case xsql.Collection:
				valCopy = vt.Clone()
			case xsql.Row:
				valCopy = vt.Clone()
			}
		} else {
			valCopy = val
		}
		i++
		// Fallback to set the context when sending out so that all children have the same parent ctx
		// If has set ctx in the node impl, do not override it
		if vt, ok := valCopy.(xsql.HasTracerCtx); ok && vt.GetTracerCtx() == nil {
			vt.SetTracerCtx(o.spanCtx)
		}
		// wait buffer consume if buffer full
		if o.disableBufferFullDiscard {
			select {
			case out <- valCopy:
				continue
			case <-o.ctx.Done():
				return
			}
		}
		// Try to send the latest one. If full, read the oldest one and retry
	forlabel:
		for {
			select {
			case out <- valCopy:
				break forlabel
			case <-o.ctx.Done():
				return
			default:
				// read the oldest to drop.
				oldest := <-out
				// record the error and stop propagating to avoid infinite loop
				// TODO get a unique id for the message
				o.onErrorOpt(o.ctx, fmt.Errorf("buffer full, drop message %v from %s to %s", oldest, o.name, name), false)
			}
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

func (o *defaultSinkNode) GetInput() (chan any, string) {
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
		o.ctx.GetLogger().Infof("node %s is closing", o.name)
		o.opsWg.Done()
	} else {
		o.ctx.GetLogger().Infof("node %s is missing close wg", o.name)
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
		infra.DrainError(ctx, errorx.NewEOF(), o.ctrlCh)
	}
}

// onProcessStart do the common works(metric, trace) when receiving a message from upstream
func (o *defaultNode) onProcessStart(ctx api.StreamContext, val any) {
	o.statManager.IncTotalRecordsIn()
	o.statManager.ProcessTimeStart()
	// Source just pass nil val so that no trace. The trace will start after extracting trace id
	if val != nil {
		traced, spanCtx, span := tracenode.TraceInput(ctx, val, o.name)
		if traced {
			tracenode.RecordRowOrCollection(val, span)
			o.span = span
			o.spanCtx = spanCtx
		}
	}
}

// onProcessEnd do the common works(metric, trace) after processing a message from upstream
func (o *defaultNode) onProcessEnd(ctx api.StreamContext) {
	o.statManager.ProcessTimeEnd()
	o.statManager.IncTotalMessagesProcessed(1)
	if o.span != nil {
		o.span.End()
		o.span = nil
	}
}

// onSend do the common works(metric, trace) after sending a message to downstream
func (o *defaultNode) onSend(ctx api.StreamContext, val any) {
	o.statManager.IncTotalRecordsOut()
}

// onError do the common works(metric, trace) after throwing an error
func (o *defaultNode) onError(ctx api.StreamContext, err error) {
	o.onErrorOpt(ctx, err, true)
}

// onError do the common works(metric, trace) after throwing an error
func (o *defaultNode) onErrorOpt(ctx api.StreamContext, err error, sendOut bool) {
	ctx.GetLogger().Errorf("Operation %s error: %s", ctx.GetOpId(), err)
	if sendOut && o.sendError {
		o.Broadcast(err)
	}
	if !o.isStatManagerHostBySink {
		o.statManager.IncTotalExceptions(err.Error())
	}
	if o.span != nil {
		o.span.RecordError(err)
		o.span.SetStatus(codes.Error, err.Error())
	}
}

func SourcePing(sourceType string, config map[string]any) error {
	source, err := io.Source(sourceType)
	if err != nil {
		return err
	}
	if _, ok := config["datasource"]; !ok {
		config["datasource"] = "/$$TEST_CONNECTION$$"
	}
	if pingAble, ok := source.(util.PingableConn); ok {
		return pingAble.Ping(context.Background(), config)
	}
	return fmt.Errorf("source %v doesn't support ping connection", sourceType)
}

func SinkPing(sinkType string, config map[string]any) error {
	sink, err := io.Sink(sinkType)
	if err != nil {
		return err
	}
	if pingAble, ok := sink.(util.PingableConn); ok {
		return pingAble.Ping(context.Background(), config)
	}
	return fmt.Errorf("sink %v doesn't support ping connection", sinkType)
}

func LookupPing(lookupType string, config map[string]any) error {
	lookup, err := io.LookupSource(lookupType)
	if err != nil {
		return err
	}
	if _, ok := config["datasource"]; !ok {
		config["datasource"] = "/$$TEST_CONNECTION$$"
	}
	if pingAble, ok := lookup.(util.PingableConn); ok {
		return pingAble.Ping(context.Background(), config)
	}
	return fmt.Errorf("lookup source %v doesn't support ping connection", lookupType)
}
