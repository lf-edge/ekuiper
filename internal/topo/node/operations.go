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
	"time"

	"github.com/pingcap/failpoint"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	topoContext "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/topo/node/tracenode"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
	"github.com/lf-edge/ekuiper/v2/pkg/tracer"
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
	cancelled bool
}

// New NewUnary creates *UnaryOperator value
func New(name string, options *def.RuleOption) *UnaryOperator {
	return &UnaryOperator{
		defaultSinkNode: newDefaultSinkNode(name, options),
	}
}

// SetOperation sets the executor operation
func (o *UnaryOperator) SetOperation(op UnOperation) {
	o.op = op
}

// Exec is the entry point for the executor
func (o *UnaryOperator) Exec(ctx api.StreamContext, errCh chan<- error) {
	o.prepareExec(ctx, errCh, "op")
	// validate p
	if o.concurrency < 1 {
		o.concurrency = 1
	}
	go func() {
		defer func() {
			failpoint.Inject("mockTimeConsumingClose", func() {
				time.Sleep(300 * time.Millisecond)
			})
			o.Close()
		}()
		err := infra.SafeRun(func() error {
			o.doOp(ctx.WithInstance(0), errCh)
			return nil
		})
		if err != nil {
			infra.DrainError(ctx, err, errCh)
		}
	}()
}

func (o *UnaryOperator) doOp(ctx api.StreamContext, errCh chan<- error) {
	logger := ctx.GetLogger()
	if o.op == nil {
		logger.Info("Unary operator missing operation")
		return
	}
	exeCtx, cancel := ctx.WithCancel()

	defer func() {
		logger.Infof("unary operator %s instance %d done, cancelling future items", o.name, ctx.GetInstanceId())
		cancel()
	}()

	fv, afv := xsql.NewFunctionValuersForOp(exeCtx)

	for {
		select {
		// process incoming item
		case item := <-o.input:
			data, processed := o.commonIngest(ctx, item)
			if processed {
				break
			}
			o.statManager.IncTotalRecordsIn()
			o.statManager.ProcessTimeStart()
			traced, _, span := tracenode.TraceInput(ctx, data, ctx.GetOpId())
			result := o.op.Apply(exeCtx, data, fv, afv)
			switch val := result.(type) {
			case nil:
				if traced {
					span.End()
				}
				o.statManager.IncTotalMessagesProcessed(1)
				continue
			case error:
				logger.Errorf("Operation %s error: %s", ctx.GetOpId(), val)
				if traced {
					span.End()
				}
				o.Broadcast(val)
				o.statManager.IncTotalMessagesProcessed(1)
				o.statManager.IncTotalExceptions(val.Error())
				continue
			case []xsql.Row:
				o.statManager.ProcessTimeEnd()
				if traced {
					span.End()
				}
				for _, v := range val {
					o.Broadcast(v)
					o.statManager.IncTotalMessagesProcessed(1)
					o.statManager.IncTotalRecordsOut()
				}
				o.statManager.SetBufferLength(int64(len(o.input)))
			default:
				o.statManager.ProcessTimeEnd()
				if traced {
					tracenode.RecordRowOrCollection(val, span)
					span.End()
				}
				o.Broadcast(val)
				o.statManager.IncTotalMessagesProcessed(1)
				o.statManager.IncTotalRecordsOut()
				o.statManager.SetBufferLength(int64(len(o.input)))
			}
		// is cancelling
		case <-ctx.Done():
			logger.Infof("unary operator %s instance %d cancelling....", o.name, ctx.GetInstanceId())
			cancel()
			return
		}
	}
}

func (o *UnaryOperator) traceUnarySplitRow(ctx, spanCtx api.StreamContext, row xsql.Row) {
	if !ctx.IsTraceEnabled() || row == nil {
		return
	}
	subCtx, span := tracer.GetTracer().Start(spanCtx, fmt.Sprintf("%s_split", ctx.GetOpId()))
	defer span.End()
	row.SetTracerCtx(topoContext.WithContext(subCtx))
	tracenode.RecordRowOrCollection(row, span)
}
