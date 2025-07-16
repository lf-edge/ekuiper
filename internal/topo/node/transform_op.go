// Copyright 2024-2025 EMQ Technologies Co., Ltd.
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
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"text/template"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/topo/transform"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

// TransformOp transforms the row/collection to sink tuples
// Immutable: false
// Change trigger frequency: true, by sendSingle property
// Input: Row/Collection
// Output: MessageTuple, SinkTupleList, RawTuple
type TransformOp struct {
	*defaultSinkNode
	dataField       string
	fields          []string
	excludeFields   []string
	sendSingle      bool
	omitIfEmpty     bool
	hasMetaTemplate bool
	// If the result format is text, the dataTemplate should be used to format the data and skip the encode step. Otherwise, the text must be unmarshall back to map
	isTextFormat bool
	dt           *template.Template
	templates    map[string]*template.Template
	metaStore    map[string]any
	isSliceMode  bool
	// temp state
	output bytes.Buffer
}

// NewTransformOp creates a transform node
// sink conf should have been validated before
func NewTransformOp(name string, rOpt *def.RuleOption, sc *SinkConf, templates []string) (*TransformOp, error) {
	if len(sc.Fields) > 0 && len(sc.ExcludeFields) > 0 {
		return nil, fmt.Errorf("field and excludeFields cannot both be set")
	}
	o := &TransformOp{
		defaultSinkNode: newDefaultSinkNode(name, rOpt),
		dataField:       sc.DataField,
		fields:          sc.Fields,
		excludeFields:   sc.ExcludeFields,
		sendSingle:      sc.SendSingle,
		omitIfEmpty:     sc.Omitempty,
		isTextFormat:    xsql.IsTextFormat(sc.Format),
		templates:       map[string]*template.Template{},
	}
	if rOpt.Experiment != nil && rOpt.Experiment.UseSliceTuple {
		if len(o.fields) > 0 {
			return nil, errors.New("slice tuple mode do not support sink fields yet")
		}
		if len(o.dataField) > 0 {
			return nil, errors.New("slice tuple mode do not support sink dataField yet")
		}
		o.isSliceMode = true
	}
	var (
		temp *template.Template
		err  error
	)
	if sc.DataTemplate != "" {
		if strings.Contains(sc.DataTemplate, "meta") {
			if o.metaStore == nil {
				o.metaStore = make(map[string]any, 1)
			}
			o.hasMetaTemplate = true
			temp, err = transform.GenTpWithMeta(sc.DataTemplate, o.metaStore)
		} else {
			temp, err = transform.GenTp(sc.DataTemplate)
		}
		if err != nil {
			return nil, err
		}
		o.dt = temp
	}
	for _, tstr := range templates {
		if strings.Contains(tstr, "meta") {
			if o.metaStore == nil {
				o.metaStore = make(map[string]any, 1)
			}
			o.hasMetaTemplate = true
			temp, err = transform.GenTpWithMeta(tstr, o.metaStore)
		} else {
			temp, err = transform.GenTp(tstr)
		}
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
	if t.isSliceMode {
		if t.omitIfEmpty {
			switch dt := item.(type) {
			case *xsql.SliceTuple:
				if dt.SourceContent.IsEmpty() {
					ctx.GetLogger().Debugf("receive empty result %v in sink, dropped", dt)
					return nil
				}
			case xsql.Collection:
				if dt.Len() == 0 {
					ctx.GetLogger().Debugf("receive empty result %v in sink, dropped", dt)
					return nil
				}
			}
		}
		return t.transformSlice(ctx, item)
	}
	if ic, ok := item.(xsql.Collection); ok && t.omitIfEmpty && ic.Len() == 0 {
		ctx.GetLogger().Debugf("receive empty collection, dropped")
		return nil
	}
	if t.hasMetaTemplate {
		var et int64
		if fv, ok := item.(xsql.FuncValuer); ok {
			v, _ := fv.FuncValue("event_time")
			if ett, exist := v.(int64); exist {
				et = ett
			}
		}
		t.metaStore["et"] = et
	}
	outs := itemToMap(item)
	if t.omitIfEmpty && (item == nil || len(outs) == 0) {
		ctx.GetLogger().Debugf("receive empty result %v in sink, dropped", outs)
		return nil
	}
	// MessageTuple or SinkTupleList
	var spanCtx api.StreamContext
	if input, ok := item.(xsql.HasTracerCtx); ok {
		spanCtx = input.GetTracerCtx()
	}
	var result []any
	if t.sendSingle {
		result = make([]any, 0, len(outs))
		for _, out := range outs {
			if t.omitIfEmpty && (out == nil || len(out) == 0) {
				ctx.GetLogger().Debugf("receive empty single result %v in sink, dropped", out)
				continue
			}
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

func (t *TransformOp) transformSlice(ctx api.StreamContext, item any) []any {
	var result []any
	props, err := t.calculateProps(item)
	if err != nil {
		result = []any{err}
		return result
	}
	switch dt := item.(type) {
	case *xsql.SliceTuple:
		dt.Props = props
		result = []any{dt}
	case xsql.Collection:
		pack := make([]*xsql.SliceTuple, 0, dt.Len())
		err = dt.Range(func(i int, r xsql.ReadonlyRow) (bool, error) {
			if rs, ok := r.(*xsql.SliceTuple); ok {
				rs.Props = props
				pack = append(pack, rs)
			} else {
				ctx.GetLogger().Warnf("receive non slice tuple, dropped")
			}
			return true, nil
		})
		if err != nil {
			result = []any{err}
		} else {
			result = []any{pack}
		}
	}
	return result
}

// TODO keep the tuple meta etc.
func toSinkTuple(_, spanCtx api.StreamContext, bs any, props map[string]string) any {
	if bs == nil {
		return bs
	}
	switch bt := bs.(type) {
	case []byte:
		return &xsql.RawTuple{Ctx: spanCtx, Rawdata: bt, Props: props, Timestamp: timex.GetNow()}
	case map[string]any:
		return &xsql.Tuple{Ctx: spanCtx, Message: bt, Timestamp: timex.GetNow(), Props: props}
	case []map[string]any:
		tuples := make([]api.MessageTuple, 0, len(bt))
		for _, m := range bt {
			tuples = append(tuples, &xsql.Tuple{Ctx: spanCtx, Message: m, Timestamp: timex.GetNow()})
		}
		return &xsql.TransformedTupleList{Ctx: spanCtx, Content: tuples, Maps: bt, Props: props}
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
		m, selected, e = transform.TransItem(bs, t.dataField, t.fields, t.excludeFields)
	} else {
		m, selected, e = transform.TransItem(d, t.dataField, t.fields, t.excludeFields)
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
