// Copyright 2021 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"sync"
)

// UnOperation interface represents unary operations (i.e. Map, Filter, etc)
type UnOperation interface {
	Apply(ctx api.StreamContext, data interface{}, fv *xsql.FunctionValuer, afv *xsql.AggregateFunctionValuer) interface{}
}

// UnFunc implements UnOperation as type func (context.Context, interface{})
type UnFunc func(api.StreamContext, interface{}) interface{}

// Apply implements UnOperation.Apply method
func (f UnFunc) Apply(ctx api.StreamContext, data interface{}) interface{} {
	return f(ctx, data)
}

type UnaryOperator struct {
	*defaultSinkNode
	op        UnOperation
	mutex     sync.RWMutex
	cancelled bool
}

// NewUnary creates *UnaryOperator value
func New(name string, options *api.RuleOption) *UnaryOperator {
	return &UnaryOperator{
		defaultSinkNode: &defaultSinkNode{
			input: make(chan interface{}, options.BufferLength),
			defaultNode: &defaultNode{
				name:        name,
				outputs:     make(map[string]chan<- interface{}),
				concurrency: 1,
				sendError:   options.SendError,
			},
		},
	}
}

// SetOperation sets the executor operation
func (o *UnaryOperator) SetOperation(op UnOperation) {
	o.op = op
}

// Exec is the entry point for the executor
func (o *UnaryOperator) Exec(ctx api.StreamContext, errCh chan<- error) {
	o.ctx = ctx
	log := ctx.GetLogger()
	log.Debugf("Unary operator %s is started", o.name)

	if len(o.outputs) <= 0 {
		go func() { errCh <- fmt.Errorf("no output channel found") }()
		return
	}

	// validate p
	if o.concurrency < 1 {
		o.concurrency = 1
	}
	//reset status
	o.statManagers = nil

	for i := 0; i < o.concurrency; i++ { // workers
		instance := i
		go o.doOp(ctx.WithInstance(instance), errCh)
	}
}

func (o *UnaryOperator) doOp(ctx api.StreamContext, errCh chan<- error) {
	logger := ctx.GetLogger()
	if o.op == nil {
		logger.Infoln("Unary operator missing operation")
		return
	}
	exeCtx, cancel := ctx.WithCancel()

	defer func() {
		logger.Infof("unary operator %s instance %d done, cancelling future items", o.name, ctx.GetInstanceId())
		cancel()
	}()

	stats, err := NewStatManager("op", ctx)
	if err != nil {
		o.drainError(errCh, err, ctx)
		return
	}
	o.mutex.Lock()
	o.statManagers = append(o.statManagers, stats)
	o.mutex.Unlock()
	fv, afv := xsql.NewFunctionValuersForOp(exeCtx)

	for {
		select {
		// process incoming item
		case item := <-o.input:
			processed := false
			if item, processed = o.preprocess(item); processed {
				break
			}
			stats.IncTotalRecordsIn()
			stats.ProcessTimeStart()
			result := o.op.Apply(exeCtx, item, fv, afv)

			switch val := result.(type) {
			case nil:
				continue
			case error:
				logger.Errorf("Operation %s error: %s", ctx.GetOpId(), val)
				o.Broadcast(val)
				stats.IncTotalExceptions()
				continue
			default:
				stats.ProcessTimeEnd()
				o.Broadcast(val)
				stats.IncTotalRecordsOut()
				stats.SetBufferLength(int64(len(o.input)))
			}
		// is cancelling
		case <-ctx.Done():
			logger.Infof("unary operator %s instance %d cancelling....", o.name, ctx.GetInstanceId())
			o.mutex.Lock()
			cancel()
			o.cancelled = true
			o.mutex.Unlock()
			return
		}
	}
}

func (o *UnaryOperator) drainError(errCh chan<- error, err error, ctx api.StreamContext) {
	go func() {
		select {
		case errCh <- err:
			ctx.GetLogger().Errorf("unary operator %s error %s", o.name, err)
		case <-ctx.Done():
			// stop waiting
		}
	}()
}
