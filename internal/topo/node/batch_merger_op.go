// Copyright 2025 EMQ Technologies Co., Ltd.
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

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
)

type BatchMergerOp struct {
	*defaultSinkNode
	// save lastRow to get the props
	lastRow any
	wt      *xsql.WindowTuples
}

func NewBatchMergerOp(name string, rOpt *def.RuleOption) (*BatchMergerOp, error) {
	return &BatchMergerOp{
		defaultSinkNode: newDefaultSinkNode(name, rOpt),
	}, nil
}

// Exec decode op receives map/[]map and converts it to bytes.
// If receiving bytes, just return it.
func (o *BatchMergerOp) Exec(ctx api.StreamContext, errCh chan<- error) {
	o.prepareExec(ctx, errCh, "op")
	go func() {
		defer func() {
			o.Close()
		}()
		err := infra.SafeRun(func() error {
			count := 0
			for {
				select {
				case <-ctx.Done():
					ctx.GetLogger().Infof("batch writer node %s is finished", o.name)
					return nil
				case item := <-o.input:
					data, processed := o.ingest(ctx, item)
					if processed {
						break
					}
					switch dt := data.(type) {
					case xsql.BatchEOFTuple:
						if count > 0 {
							o.Broadcast(o.wt)
							o.onSend(ctx, o.wt)
							count = 0
							o.lastRow = nil
							o.wt = nil
						}
					case *xsql.SliceTuple:
						o.onProcessStart(ctx, data)
						o.appendWindowTuples(dt)
						o.onProcessEnd(ctx)
						count++
					case xsql.Row:
						o.onProcessStart(ctx, data)
						o.appendWindowTuples(dt)
						o.onProcessEnd(ctx)
						o.lastRow = dt
						count++
					case api.MessageTupleList:
						o.onProcessStart(ctx, data)
						// TODO: find a way to avoid using ToMaps
						for _, m := range dt.ToMaps() {
							row := &xsql.Tuple{
								Message: m,
							}
							o.appendWindowTuples(row)
						}
						o.onProcessEnd(ctx)
						o.lastRow = dt
						count++
					default:
						o.onError(ctx, fmt.Errorf("unknown data type: %T", data))
					}
				}
			}
		})
		if err != nil {
			infra.DrainError(ctx, err, errCh)
		}
	}()
}

func (o *BatchMergerOp) ingest(ctx api.StreamContext, item any) (any, bool) {
	ctx.GetLogger().Debugf("receive %v", item)
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
	return item, false
}

func (o *BatchMergerOp) appendWindowTuples(row xsql.Row) {
	if o.wt == nil {
		o.wt = &xsql.WindowTuples{
			Content: make([]xsql.Row, 0),
		}
	}
	o.wt.Content = append(o.wt.Content, row)
	o.lastRow = row
}
