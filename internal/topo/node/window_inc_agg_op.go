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
	"fmt"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	topoContext "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/topo/state"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

type WindowIncAggOperator struct {
	*defaultSinkNode
	windowConfig *WindowConfig
	Dimensions   ast.Dimensions
	aggFields    []*ast.Field
	windowExec   windowIncAggExec
}

func NewWindowIncAggOp(name string, w *WindowConfig, dimensions ast.Dimensions, aggFields []*ast.Field, options *def.RuleOption) (*WindowIncAggOperator, error) {
	o := new(WindowIncAggOperator)
	o.defaultSinkNode = newDefaultSinkNode(name, options)
	o.windowConfig = w
	o.Dimensions = dimensions
	o.aggFields = aggFields
	switch w.Type {
	case ast.COUNT_WINDOW:
		wExec := &CountWindowIncAggOp{
			WindowIncAggOperator: o,
			windowSize:           w.CountLength,
		}
		o.windowExec = wExec
	}
	return o, nil
}

func (o *WindowIncAggOperator) Close() {
	o.defaultNode.Close()
}

// Exec is the entry point for the executor
// input: *xsql.Tuple from preprocessor
// output: xsql.WindowTuplesSet
func (o *WindowIncAggOperator) Exec(ctx api.StreamContext, errCh chan<- error) {
	o.prepareExec(ctx, errCh, "op")
	go func() {
		defer o.Close()
		err := infra.SafeRun(func() error {
			o.windowExec.exec(ctx, errCh)
			return nil
		})
		if err != nil {
			infra.DrainError(ctx, err, errCh)
		}
	}()
}

type windowIncAggExec interface {
	exec(ctx api.StreamContext, errCh chan<- error)
}

type CountWindowIncAggOp struct {
	*WindowIncAggOperator
	windowSize int

	currWindow     *IncAggWindow
	currWindowSize int
}

type IncAggWindow struct {
	DimensionsIncAggRange map[string]*IncAggRange
}

type IncAggRange struct {
	fv      *xsql.FunctionValuer
	lastRow *xsql.Tuple
	fields  map[string]interface{}
}

func (co *CountWindowIncAggOp) exec(ctx api.StreamContext, errCh chan<- error) {
	fv, _ := xsql.NewFunctionValuersForOp(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case input := <-co.input:
			data, processed := co.commonIngest(ctx, input)
			if processed {
				continue
			}
			co.onProcessStart(ctx, input)
			switch row := data.(type) {
			case *xsql.Tuple:
				if co.currWindow == nil {
					co.setIncAggWindow(ctx)
				}
				name := bytes.NewBufferString("dim_")
				ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(row, fv, &xsql.WildcardValuer{Data: row})}
				for _, d := range co.Dimensions {
					r := ve.Eval(d.Expr)
					if _, ok := r.(error); ok {
						continue
					} else {
						name.WriteString(fmt.Sprintf("%v,", r))
					}
				}
				co.incAggCal(ctx, name.String(), row, co.currWindow)
				co.currWindowSize++
				if co.currWindowSize >= co.windowSize {
					co.emit(ctx, errCh)
				}
			}
			co.onProcessEnd(ctx)
		}
		co.statManager.SetBufferLength(int64(len(co.input)))
	}
}

func (co *CountWindowIncAggOp) setIncAggWindow(ctx api.StreamContext) {
	co.currWindow = &IncAggWindow{
		DimensionsIncAggRange: make(map[string]*IncAggRange),
	}
}

func (co *CountWindowIncAggOp) newIncAggRange(ctx api.StreamContext) *IncAggRange {
	fstore, _ := state.CreateStore("incAggWindow", 0)
	fctx := topoContext.Background().WithMeta(ctx.GetRuleId(), ctx.GetOpId(), fstore)
	fv, _ := xsql.NewFunctionValuersForOp(fctx)
	return &IncAggRange{
		fv:     fv,
		fields: make(map[string]interface{}),
	}
}

func (co *CountWindowIncAggOp) incAggCal(ctx api.StreamContext, dimension string, row *xsql.Tuple, incAggWindow *IncAggWindow) {
	dimensionsRange, ok := incAggWindow.DimensionsIncAggRange[dimension]
	if !ok {
		dimensionsRange = co.newIncAggRange(ctx)
		incAggWindow.DimensionsIncAggRange[dimension] = dimensionsRange
	}
	ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(dimensionsRange.fv, row, &xsql.WildcardValuer{Data: row})}
	dimensionsRange.lastRow = row
	for _, aggField := range co.aggFields {
		vi := ve.Eval(aggField.Expr)
		colName := aggField.Name
		if len(aggField.AName) > 0 {
			colName = aggField.AName
		}
		dimensionsRange.fields[colName] = vi
	}
}

func (co *CountWindowIncAggOp) emit(ctx api.StreamContext, errCh chan<- error) {
	results := &xsql.WindowTuples{
		Content: make([]xsql.Row, 0),
	}
	for _, incAggRange := range co.currWindow.DimensionsIncAggRange {
		for name, value := range incAggRange.fields {
			incAggRange.lastRow.Set(name, value)
		}
		results.Content = append(results.Content, incAggRange.lastRow)
	}
	results.WindowRange = xsql.NewWindowRange(timex.GetNowInMilli(), timex.GetNowInMilli())
	co.currWindowSize = 0
	co.currWindow = nil
	co.Broadcast(results)
}
