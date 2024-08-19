// Copyright 2024 EMQ Technologies Co., Ltd.
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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"text/template"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"go.opentelemetry.io/otel/trace"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	topoContext "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/topo/node/tracenode"
	"github.com/lf-edge/ekuiper/v2/internal/topo/transform"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
	"github.com/lf-edge/ekuiper/v2/pkg/tracer"
)

// TransformOp transforms the row/collection to sink tuples
// Immutable: false
// Change trigger frequency: true, by sendSingle property
// Input: Row/Collection
// Output: MessageTuple, SinkTupleList, RawTuple
type TransformOp struct {
	*defaultSinkNode
	dataField   string
	fields      []string
	sendSingle  bool
	omitIfEmpty bool
	// If the result format is text, the dataTemplate should be used to format the data and skip the encode step. Otherwise, the text must be unmarshall back to map
	isTextFormat bool
	dt           *template.Template
	templates    map[string]*template.Template
	// temp state
	output bytes.Buffer
}

// NewTransformOp creates a transform node
// sink conf should have been validated before
func NewTransformOp(name string, rOpt *def.RuleOption, sc *SinkConf, templates []string) (*TransformOp, error) {
	o := &TransformOp{
		defaultSinkNode: newDefaultSinkNode(name, rOpt),
		dataField:       sc.DataField,
		fields:          sc.Fields,
		sendSingle:      sc.SendSingle,
		omitIfEmpty:     sc.Omitempty,
		isTextFormat:    xsql.IsTextFormat(sc.Format),
		templates:       map[string]*template.Template{},
	}
	if sc.DataTemplate != "" {
		temp, err := transform.GenTp(sc.DataTemplate)
		if err != nil {
			return nil, err
		}
		o.dt = temp
	}
	for _, tstr := range templates {
		temp, err := transform.GenTp(tstr)
		if err != nil {
			return nil, err
		}
		o.templates[tstr] = temp
	}
	return o, nil
}

func (t *TransformOp) Exec(ctx api.StreamContext, errCh chan<- error) {
	t.prepareExec(ctx, errCh, "op")
	go func() {
		defer func() {
			t.Close()
		}()
		err := infra.SafeRun(func() error {
			runWithOrder(ctx, t.defaultSinkNode, t.concurrency, t.Worker)
			return nil
		})
		if err != nil {
			infra.DrainError(ctx, err, errCh)
		}
	}()
}

// Worker do not need to process error and control messages
func (t *TransformOp) Worker(ctx api.StreamContext, item any) []any {
	t.statManager.ProcessTimeStart()
	defer t.statManager.ProcessTimeEnd()
	if ic, ok := item.(xsql.Collection); ok && t.omitIfEmpty && ic.Len() == 0 {
		ctx.GetLogger().Debugf("receive empty collection, dropped")
		return nil
	}
	traced, spanCtx, span := tracenode.TraceInput(ctx, item, "transform_op")
	outs := itemToMap(item)
	if traced {
		tracenode.RecordRowOrCollection(item, span)
		span.End()
	}
	if t.omitIfEmpty && (item == nil || len(outs) == 0) {
		ctx.GetLogger().Debugf("receive empty result %v in sink, dropped", outs)
		return nil
	}
	// MessageTuple or SinkTupleList
	var result []any
	if t.sendSingle {
		result = make([]any, 0, len(outs))
		for _, out := range outs {
			props, err := t.calculateProps(out)
			if err != nil {
				result = append(result, err)
				continue
			}
			bs, err := t.doTransform(out)
			if err != nil {
				result = append(result, err)
			} else {
				result = append(result, toSinkTuple(ctx, spanCtx, bs, props))
			}
		}
	} else {
		props, err := t.calculateProps(outs)
		if err != nil {
			result = append(result, err)
		} else {
			bs, err := t.doTransform(outs)
			if err != nil {
				result = append(result, err)
			} else {
				result = append(result, toSinkTuple(ctx, spanCtx, bs, props))
			}
		}
	}
	return result
}

// TODO keep the tuple meta etc.
func toSinkTuple(ctx, spanCtx api.StreamContext, bs any, props map[string]string) any {
	if bs == nil {
		return bs
	}
	var span trace.Span
	var sctx context.Context
	if ctx.IsTraceEnabled() {
		sctx, span = tracer.GetTracer().Start(spanCtx, "transform_op_split")
		defer span.End()
	}
	switch bt := bs.(type) {
	case []byte:
		return &xsql.RawTuple{Ctx: topoContext.WithContext(sctx), Rawdata: bt, Props: props, Timestamp: timex.GetNow()}
	case map[string]any:
		return &xsql.Tuple{Ctx: topoContext.WithContext(sctx), Message: bt, Timestamp: timex.GetNow(), Props: props}
	case []map[string]any:
		tuples := make([]api.MessageTuple, 0, len(bt))
		for _, m := range bt {
			tuples = append(tuples, &xsql.Tuple{Ctx: topoContext.WithContext(sctx), Message: m, Timestamp: timex.GetNow()})
		}
		return &xsql.TransformedTupleList{Ctx: topoContext.WithContext(sctx), Content: tuples, Maps: bt, Props: props}
	default:
		return fmt.Errorf("invalid transform result type %v", bs)
	}
}

// doTransform transforms the data according to the dataTemplate and fields
// If the dataTemplate is the last action and the result is text, the data will be returned as []byte
// Otherwise, the data will be return as a map or []map
func (t *TransformOp) doTransform(d any) (any, error) {
	var (
		bs          []byte
		transformed bool
		selected    bool
		m           any
		e           error
	)
	if t.dt != nil {
		var output bytes.Buffer
		err := t.dt.Execute(&output, d)
		if err != nil {
			return nil, fmt.Errorf("fail to encode data %v with dataTemplate for error %v", d, err)
		}
		bs = output.Bytes()
		transformed = true
	}

	if transformed {
		m, selected, e = transform.TransItem(bs, t.dataField, t.fields)
	} else {
		m, selected, e = transform.TransItem(d, t.dataField, t.fields)
	}
	if e != nil {
		return nil, fmt.Errorf("fail to TransItem data %v for error %v", d, e)
	}
	// if only do data template
	if transformed && !selected {
		if t.isTextFormat {
			return bs, nil
		} else {
			err := json.Unmarshal(bs, &m)
			if err != nil {
				return nil, fmt.Errorf("fail to decode data %s after applying dataTemplate for error %v", string(bs), err)
			}
			return m, nil
		}
	}
	return m, nil
}

func (t *TransformOp) calculateProps(data any) (map[string]string, error) {
	if len(t.templates) == 0 {
		return nil, nil
	}
	result := make(map[string]string, len(t.templates))
	for k, temp := range t.templates {
		err := temp.Execute(&t.output, data)
		if err != nil {
			return nil, fmt.Errorf("fail to calculate props %s through data %v with dataTemplate for error %v", k, data, err)
		}
		result[k] = t.output.String()
		t.output.Reset()
	}
	return result, nil
}

func itemToMap(item interface{}) []map[string]any {
	var outs []map[string]any
	switch val := item.(type) {
	case error:
		outs = []map[string]any{
			{"error": val.Error()},
		}
		break
	case xsql.Collection: // The order is important here, because some element is both a collection and a row, such as WindowTuples, JoinTuples, etc.
		maps := val.ToMaps()
		outs = make([]map[string]any, len(maps))
		for i, m := range maps {
			outs[i] = m
		}
		break
	case xsql.Row:
		outs = []map[string]any{
			val.ToMap(),
		}
		break
	default:
		outs = []map[string]any{
			{"error": fmt.Sprintf("result is not a map slice but found %#v", val)},
		}
	}
	return outs
}
