// Copyright 2022-2025 EMQ Technologies Co., Ltd.
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
	Funcs       []*ast.Call
	FieldFuncs  []*ast.Call
	transformed bool
}

func (p *AnalyticFuncsOp) evalTupleFunc(calls []*ast.Call, ve *xsql.ValuerEval, input xsql.Row) (xsql.Row, error) {
	for _, call := range calls {
		f := call
		result := ve.Eval(f)
		if e, ok := result.(error); ok {
			return nil, e
		}
		if iv, ok := input.(model.IndexValuer); ok {
			iv.SetTempByIndex(f.CacheIndex, result)
		} else {
			input.Set(f.CachedField, result)
		}
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
			if iv, ok := row.(model.IndexValuer); ok {
				iv.SetTempByIndex(f.CacheIndex, result)
			} else {
				row.Set(f.CachedField, result)
			}
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	return input, nil
}

func (p *AnalyticFuncsOp) Apply(ctx api.StreamContext, data interface{}, fv *xsql.FunctionValuer, _ *xsql.AggregateFunctionValuer) (got interface{}) {
	ctx.GetLogger().Debugf("AnalyticFuncsOp receive: %v", data)
	if !p.transformed {
		newF := make([]*ast.Call, len(p.Funcs))
		for i, f := range p.Funcs {
			newF[i] = &ast.Call{
				Name:        f.Name,
				FuncId:      f.FuncId,
				FuncType:    f.FuncType,
				Args:        f.Args,
				CachedField: f.CachedField,
				CacheIndex:  f.CacheIndex,
				Partition:   f.Partition,
				WhenExpr:    f.WhenExpr,
			}
		}
		p.Funcs = newF
		newFF := make([]*ast.Call, len(p.FieldFuncs))
		for i, f := range p.FieldFuncs {
			newFF[i] = &ast.Call{
				Name:        f.Name,
				FuncId:      f.FuncId,
				FuncType:    f.FuncType,
				Args:        f.Args,
				CachedField: f.CachedField,
				CacheIndex:  f.CacheIndex,
				Partition:   f.Partition,
				WhenExpr:    f.WhenExpr,
			}
		}
		p.FieldFuncs = newFF
		p.transformed = true
	}
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
