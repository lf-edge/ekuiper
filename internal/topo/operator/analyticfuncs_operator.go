// Copyright 2022-2026 EMQ Technologies Co., Ltd.
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

package operator

import (
	"fmt"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
)

type AnalyticFuncsOp struct {
	Funcs      []*ast.Call
	FieldFuncs []*ast.Call

	initialized bool
	lead        *leadBuffer
}

func setAnalyticValue(input xsql.Row, call *ast.Call, value interface{}) {
	if iv, ok := input.(model.IndexValuer); ok && call.CacheIndex >= 0 {
		iv.SetTempByIndex(call.CacheIndex, value)
	} else {
		input.Set(call.CachedField, value)
	}
}

func cloneAnalyticCalls(calls []*ast.Call) []*ast.Call {
	cloned := make([]*ast.Call, len(calls))
	for i, f := range calls {
		cloned[i] = &ast.Call{
			Name:        f.Name,
			FuncId:      f.FuncId,
			FuncType:    f.FuncType,
			Args:        f.Args,
			CachedField: f.CachedField,
			CacheIndex:  f.CacheIndex,
			Partition:   f.Partition,
			WhenExpr:    f.WhenExpr,
			UntilExpr:   f.UntilExpr,
		}
	}
	return cloned
}

func (p *AnalyticFuncsOp) init(ctx api.StreamContext) error {
	if p.initialized {
		return nil
	}
	var leads []*ast.Call
	groups := make([][]*ast.Call, 2)
	seen := make(map[int]bool)
	// Preserve the existing field-function-before-function evaluation order.
	for i, calls := range [][]*ast.Call{p.FieldFuncs, p.Funcs} {
		for _, call := range cloneAnalyticCalls(calls) {
			if call.Name != "lead" {
				groups[i] = append(groups[i], call)
			} else if !seen[call.FuncId] {
				seen[call.FuncId] = true
				leads = append(leads, call)
			}
		}
	}
	if len(leads) > 0 {
		buffer := &leadBuffer{calls: leads, requests: make(map[int]map[string][]*leadRequest)}
		if err := buffer.restore(ctx); err != nil {
			return err
		}
		p.lead = buffer
	}
	p.FieldFuncs, p.Funcs = groups[0], groups[1]
	p.initialized = true
	return nil
}

func evalImmediate(calls []*ast.Call, ve *xsql.ValuerEval, row xsql.Row) error {
	for _, call := range calls {
		result := ve.Eval(call)
		if err, ok := result.(error); ok {
			return err
		}
		setAnalyticValue(row, call, result)
	}
	return nil
}

func (p *AnalyticFuncsOp) Apply(ctx api.StreamContext, data interface{}, fv *xsql.FunctionValuer, _ *xsql.AggregateFunctionValuer) interface{} {
	ctx.GetLogger().Debugf("AnalyticFuncsOp receive: %v", data)
	if err := p.init(ctx); err != nil {
		return err
	}
	switch input := data.(type) {
	case error:
		return input
	case xsql.Row:
		ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(input, fv)}
		for _, calls := range [][]*ast.Call{p.FieldFuncs, p.Funcs} {
			if err := evalImmediate(calls, ve, input); err != nil {
				return err
			}
		}
		if p.lead != nil {
			return p.lead.apply(ctx, input, fv)
		}
		return input
	case xsql.Collection:
		if p.lead != nil {
			return fmt.Errorf("lead is not supported on collections")
		}
		// Preserve the two passes: field functions over the whole collection,
		// followed by the remaining functions over the whole collection.
		for _, calls := range [][]*ast.Call{p.FieldFuncs, p.Funcs} {
			err := input.RangeSet(func(_ int, row xsql.Row) (bool, error) {
				ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(row, &xsql.WindowRangeValuer{WindowRange: input.GetWindowRange()}, fv, &xsql.WildcardValuer{Data: row})}
				err := evalImmediate(calls, ve, row)
				return err == nil, err
			})
			if err != nil {
				return err
			}
		}
		return input
	default:
		return fmt.Errorf("run analytic funcs op error: invalid input %[1]T(%[1]v)", input)
	}
}

func (p *AnalyticFuncsOp) Finalize(ctx api.StreamContext, _ interface{}, _ *xsql.FunctionValuer, _ *xsql.AggregateFunctionValuer) interface{} {
	if err := p.init(ctx); err != nil {
		return err
	}
	if p.lead == nil {
		return nil
	}
	return p.lead.finalize(ctx)
}

func (p *AnalyticFuncsOp) Watermark(ctx api.StreamContext, marker *xsql.WatermarkTuple) (*xsql.WatermarkTuple, error) {
	if err := p.init(ctx); err != nil {
		return nil, err
	}
	if p.lead == nil {
		return marker, nil
	}
	return p.lead.watermark(ctx, marker)
}
