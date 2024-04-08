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
	"text/template"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/infra"
)

type TransformOp struct {
	*defaultSinkNode
	dataField   string
	fields      []string
	sendSingle  bool
	omitIfEmpty bool
	dt          *template.Template
}

// NewTransformOp creates a transform node
// sink conf should have been validated before
func NewTransformOp(name string, rOpt *api.RuleOption, sc *SinkConf) (*TransformOp, error) {
	o := &TransformOp{
		defaultSinkNode: newDefaultSinkNode(name, rOpt),
		dataField:       sc.DataField,
		fields:          sc.Fields,
		sendSingle:      sc.SendSingle,
		omitIfEmpty:     sc.Omitempty,
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
	t.prepareExec(ctx)
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
func (t *TransformOp) Worker(item any) []any {
	t.statManager.IncTotalRecordsIn()
	t.statManager.ProcessTimeStart()
	if ic, ok := item.(xsql.Collection); ok && t.omitIfEmpty && ic.Len() == 0 {
		return nil
	}
	var m map[string]any
	switch input := item.(type) {
	case xsql.Row:
		m = input.ToMap()
	case xsql.Collection:
		// omit empty data
		if t.omitIfEmpty && input.Len() == 0 {
			return nil
		}
		_ = input.Range(func(i int, r xsql.ReadonlyRow) (bool, error) {
			b.buffer.AddTuple(r.(xsql.Row))
			return true, nil
		})
	default:
		ctx.GetLogger().Errorf("run batch error: invalid data type %T", input)
	}
}
