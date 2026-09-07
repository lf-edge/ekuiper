// Copyright 2021-2026 EMQ Technologies Co., Ltd.
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
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/pingcap/failpoint"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
)

// UnOperation interface represents unary operations (i.e. Map, Filter, etc)
type UnOperation interface {
	Apply(ctx api.StreamContext, data interface{}, fv *xsql.FunctionValuer, afv *xsql.AggregateFunctionValuer) interface{}
}

// FinalizableOperation can release buffered rows before an EOF marker is sent
// downstream. Cancellation is intentionally not treated as finalization.
type FinalizableOperation interface {
	Finalize(ctx api.StreamContext, marker interface{}, fv *xsql.FunctionValuer, afv *xsql.AggregateFunctionValuer) interface{}
}

// WatermarkOperation constrains event-time progress while an operation buffers rows.
type WatermarkOperation interface {
	Watermark(ctx api.StreamContext, marker *xsql.WatermarkTuple) (*xsql.WatermarkTuple, error)
}

// SnapshotOperation materializes state only when a checkpoint is triggered.
type SnapshotOperation interface {
	Snapshot(ctx api.StreamContext) error
}

func (o *UnaryOperator) PrepareCheckpoint() error {
	if op, ok := o.op.(SnapshotOperation); ok {
		return op.Snapshot(o.ctx)
	}
	return nil
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
	done := ctx.Done()
	emitResult := func(result interface{}) {
		switch val := result.(type) {
		case nil:
		case error:
			o.onError(ctx, val)
		case []xsql.Row:
			for _, v := range val {
				o.Broadcast(v)
				o.onSend(ctx, v)
			}
		default:
			o.Broadcast(val)
			o.onSend(ctx, val)
		}
	}

	for {
		select {
		// process incoming item
		case item := <-o.input:
			data, processed := o.commonIngestWithControl(ctx, item, func(marker interface{}) bool {
				if watermark, ok := marker.(*xsql.WatermarkTuple); ok {
					if handler, ok := o.op.(WatermarkOperation); ok {
						adjusted, err := handler.Watermark(exeCtx, watermark)
						if err != nil {
							o.onError(ctx, err)
						} else if adjusted != nil {
							o.Broadcast(adjusted)
						}
						return true
					}
				}
				if finalizer, ok := o.op.(FinalizableOperation); ok {
					switch marker.(type) {
					case xsql.EOFTuple, xsql.BatchEOFTuple:
						emitResult(finalizer.Finalize(exeCtx, marker, fv, afv))
					}
				}
				return false
			})
			if processed {
				break
			}
			o.onProcessStart(ctx, data)
			emitResult(o.op.Apply(exeCtx, data, fv, afv))
			o.onProcessEnd(ctx)
			o.statManager.SetBufferLength(int64(len(o.input)))
		// is cancelling
		case <-done:
			logger.Infof("unary operator %s instance %d cancelling....", o.name, ctx.GetInstanceId())
			cancel()
			return
		}
	}
}
