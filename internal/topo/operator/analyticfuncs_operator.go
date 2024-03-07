// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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

	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
)

type AnalyticFuncsOp struct {
	Funcs      []*ast.Call
	FieldFuncs []*ast.Call
}

func (p *AnalyticFuncsOp) evalTupleFunc(calls []*ast.Call, ve *xsql.ValuerEval, input xsql.Row) (xsql.Row, error) {
	for _, call := range calls {
		f := call
		result := ve.Eval(f)
		if e, ok := result.(error); ok {
			return nil, e
		}
		input.Set(f.CachedField, result)
	}
	return input, nil
}

func (p *AnalyticFuncsOp) evalCollectionFunc(calls []*ast.Call, fv *xsql.FunctionValuer, input xsql.Collection) (xsql.Collection, error) {
	err := input.RangeSet(func(_ int, row xsql.Row) (bool, error) {
		ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(row, &xsql.WindowRangeValuer{WindowRange: input.GetWindowRange()}, fv, &xsql.WildcardValuer{Data: row})}
		for _, call := range calls {
			f := call
			result := ve.Eval(f)
			if e, ok := result.(error); ok {
				return false, e
			}
			row.Set(f.CachedField, result)
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	return input, nil
}

func (p *AnalyticFuncsOp) Apply(ctx api.StreamContext, data interface{}, fv *xsql.FunctionValuer, _ *xsql.AggregateFunctionValuer) interface{} {
	ctx.GetLogger().Debugf("AnalyticFuncsOp receive: %v", data)
	var err error
	switch input := data.(type) {
	case error:
		return input
	case xsql.Row:
		ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(input, fv)}
		input, err = p.evalTupleFunc(p.FieldFuncs, ve, input)
		if err != nil {
			return err
		}
		input, err = p.evalTupleFunc(p.Funcs, ve, input)
		if err != nil {
			return err
		}
		data = input
	case xsql.Collection:
		input, err = p.evalCollectionFunc(p.FieldFuncs, fv, input)
		if err != nil {
			return err
		}
		input, err = p.evalCollectionFunc(p.Funcs, fv, input)
		if err != nil {
			return err
		}
		data = input
	default:
		return fmt.Errorf("run analytic funcs op error: invalid input %[1]T(%[1]v)", input)
	}
	return data
}
