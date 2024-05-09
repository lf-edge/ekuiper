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
	"encoding/json"
	"fmt"
	"text/template"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/conf"
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
	dataField   string
	fields      []string
	sendSingle  bool
	omitIfEmpty bool
	// If the result format is text, the dataTemplate should be used to format the data and skip the encode step. Otherwise, the text must be unmarshall back to map
	isTextFormat bool
	dt           *template.Template
}

// NewTransformOp creates a transform node
// sink conf should have been validated before
func NewTransformOp(name string, rOpt *def.RuleOption, sc *SinkConf) (*TransformOp, error) {
	o := &TransformOp{
		defaultSinkNode: newDefaultSinkNode(name, rOpt),
		dataField:       sc.DataField,
		fields:          sc.Fields,
		sendSingle:      sc.SendSingle,
		omitIfEmpty:     sc.Omitempty,
		isTextFormat:    xsql.IsTextFormat(sc.Format),
	}
	if sc.DataTemplate != "" {
		temp, err := template.New(name).Funcs(conf.FuncMap).Parse(sc.DataTemplate)
		if err != nil {
			return nil, err
		}
		o.dt = temp
	}
	return o, nil
}

func (t *TransformOp) Exec(ctx api.StreamContext, errCh chan<- error) {
	t.prepareExec(ctx, errCh, "op")
	go func() {
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
	outs := itemToMap(item)
	if t.omitIfEmpty && (item == nil || len(outs) == 0) {
		ctx.GetLogger().Debugf("receive empty result %v in sink, dropped", outs)
		return nil
	}
	// MessageTuple or SinkTupleList
	var result []any
	if t.sendSingle {
		result = make([]any, 0, len(outs))
		for _, out := range outs {
			bs, err := t.doTransform(out)
			if err != nil {
				result = append(result, err)
			} else {
				result = append(result, toSinkTuple(bs))
			}
		}
	} else {
		bs, err := t.doTransform(outs)
		if err != nil {
			result = append(result, err)
		} else {
			result = append(result, toSinkTuple(bs))
		}
	}
	return result
}

// TODO keep the tuple meta etc.
func toSinkTuple(bs any) any {
	if bs == nil {
		return bs
	}
	switch bt := bs.(type) {
	case map[string]any:
		return &xsql.Tuple{Message: bt, Timestamp: timex.GetNowInMilli()}
	case []map[string]any:
		tuples := make([]api.MessageTuple, 0, len(bt))
		for _, m := range bt {
			tuples = append(tuples, &xsql.Tuple{Message: m, Timestamp: timex.GetNowInMilli()})
		}
		return &xsql.MemTupleList{Content: tuples, Maps: bt}
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
	case []map[string]any: // for test only
		outs = val
		break
	default:
		outs = []map[string]any{
			{"error": fmt.Sprintf("result is not a map slice but found %#v", val)},
		}
	}
	return outs
}
