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

	"github.com/lf-edge/ekuiper/internal/converter"
	"github.com/lf-edge/ekuiper/internal/topo/node/metric"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/infra"
	"github.com/lf-edge/ekuiper/pkg/message"
)

type DecodeOp struct {
	*defaultSinkNode
	converter message.Converter
}

func NewDecodeOp(name string, ruleId string, rOpt *api.RuleOption, options *ast.Options, isWildcard, isSchemaless bool, schema map[string]*ast.JsonStreamField) (*DecodeOp, error) {
	options.Schema = nil
	options.IsWildCard = isWildcard
	options.IsSchemaLess = isSchemaless
	if schema != nil {
		options.Schema = schema
		options.StreamName = name
	}
	options.RuleID = ruleId
	converterTool, err := converter.GetOrCreateConverter(options)
	if err != nil {
		msg := fmt.Sprintf("cannot get converter from format %s, schemaId %s: %v", options.FORMAT, options.SCHEMAID, err)
		return nil, fmt.Errorf(msg)
	}
	return &DecodeOp{
		defaultSinkNode: newDefaultSinkNode(name, rOpt),
		converter:       converterTool,
	}, nil
}

// Exec decode op receives raw data and converts it to message
func (o *DecodeOp) Exec(ctx api.StreamContext, errCh chan<- error) {
	// TODO move this to new
	ctx.GetLogger().Infof("decode op started")
	o.statManager = metric.NewStatManager(ctx, "op")
	o.ctx = ctx
	go func() {
		err := infra.SafeRun(func() error {
			for {
				o.statManager.SetBufferLength(int64(len(o.input)))
				select {
				case <-ctx.Done():
					ctx.GetLogger().Infof("decode node %s is finished", o.name)
					return nil
				case item := <-o.input:
					processed := false
					if item, processed = o.preprocess(item); processed {
						break
					}
					switch d := item.(type) {
					case error:
						o.Broadcast(d)
						o.statManager.IncTotalExceptions(d.Error())
					case *xsql.Tuple:
						o.statManager.IncTotalRecordsIn()
						// Start the first event processing.
						// Later a series of events may send out in order
						o.statManager.ProcessTimeStart()
						result, err := o.converter.Decode(d.Raw)
						if err != nil {
							o.Broadcast(err)
							o.statManager.IncTotalExceptions(err.Error())
							break
						}
						switch r := result.(type) {
						case map[string]interface{}:
							d.Message = r
							d.Raw = nil
							o.Broadcast(d)
							o.statManager.IncTotalRecordsOut()
						case []map[string]interface{}:
							for _, v := range r {
								o.sendMap(v, d)
							}
						case []interface{}:
							for _, v := range r {
								if vc, ok := v.(map[string]interface{}); ok {
									o.sendMap(vc, d)
								} else {
									e := fmt.Errorf("only map[string]any inside a list is supported but got: %v", v)
									o.Broadcast(e)
									o.statManager.IncTotalExceptions(e.Error())
								}
							}
						default:
							e := fmt.Errorf("unsupported decode result: %v", r)
							o.Broadcast(e)
							o.statManager.IncTotalExceptions(e.Error())
						}
					default:
						e := fmt.Errorf("unsupported data received: %v", d)
						o.Broadcast(e)
						o.statManager.IncTotalExceptions(e.Error())
					}
					o.statManager.ProcessTimeEnd()
					o.statManager.IncTotalMessagesProcessed(1)
				}
			}
		})
		if err != nil {
			infra.DrainError(ctx, err, errCh)
		}
	}()
}

func (o *DecodeOp) sendMap(v map[string]any, d *xsql.Tuple) {
	o.Broadcast(&xsql.Tuple{
		Message:   v,
		Metadata:  d.Metadata,
		Timestamp: d.Timestamp,
		Emitter:   d.Emitter,
	})
	o.statManager.IncTotalRecordsOut()
}
