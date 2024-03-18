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

	"github.com/lf-edge/ekuiper/internal/conf"
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

func (o *DecodeOp) AttachSchema(ctx api.StreamContext, dataSource string, schema map[string]*ast.JsonStreamField, isWildcard bool) {
	ctx.GetLogger().Infof("attach schema to shared stream")
	if fastDecoder, ok := o.converter.(message.SchemaMergeAbleConverter); ok {
		if err := fastDecoder.MergeSchema(ctx.GetRuleId(), dataSource, schema, isWildcard); err != nil {
			ctx.GetLogger().Warnf("merge schema to shared stream failed, err: %v", err)
		}
	}
}

func (o *DecodeOp) DetachSchema(ruleId string) {
	conf.Log.Infof("detach schema for shared stream rule %v", ruleId)
	if fastDecoder, ok := o.converter.(message.SchemaMergeAbleConverter); ok {
		if err := fastDecoder.DetachSchema(ruleId); err != nil {
			conf.Log.Warnf("detach schema for shared stream rule %v failed, err:%v", ruleId, err)
		}
	}
}

func NewDecodeOp(name, StreamName string, ruleId string, rOpt *api.RuleOption, options *ast.Options, isWildcard, isSchemaless bool, schema map[string]*ast.JsonStreamField) (*DecodeOp, error) {
	options.Schema = nil
	options.IsWildCard = isWildcard
	options.IsSchemaLess = isSchemaless
	if schema != nil {
		options.Schema = schema
		options.StreamName = StreamName
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
			runWithOrder(ctx, o.defaultSinkNode, o.concurrency, o.Worker)
			return nil
		})
		if err != nil {
			infra.DrainError(ctx, err, errCh)
		}
	}()
}

func (o *DecodeOp) Worker(item any) []any {
	o.statManager.ProcessTimeStart()
	defer o.statManager.ProcessTimeEnd()
	switch d := item.(type) {
	case error:
		return []any{d}
	case *xsql.Tuple:
		result, err := o.converter.Decode(d.Raw)
		if err != nil {
			return []any{err}
		}
		switch r := result.(type) {
		case map[string]interface{}:
			d.Message = r
			d.Raw = nil
			return []any{d}
		case []map[string]interface{}:
			rr := make([]any, len(r))
			for i, v := range r {
				rr[i] = o.toTuple(v, d)
			}
			return rr
		case []interface{}:
			rr := make([]any, len(r))
			for i, v := range r {
				if vc, ok := v.(map[string]interface{}); ok {
					rr[i] = o.toTuple(vc, d)
				} else {
					rr[i] = fmt.Errorf("only map[string]any inside a list is supported but got: %v", v)
				}
			}
			return rr
		default:
			return []any{fmt.Errorf("unsupported decode result: %v", r)}
		}
	default:
		return []any{fmt.Errorf("unsupported data received: %v", d)}
	}
}

func (o *DecodeOp) toTuple(v map[string]any, d *xsql.Tuple) *xsql.Tuple {
	return &xsql.Tuple{
		Message:   v,
		Metadata:  d.Metadata,
		Timestamp: d.Timestamp,
		Emitter:   d.Emitter,
	}
}

var _ SchemaNode = &DecodeOp{}
