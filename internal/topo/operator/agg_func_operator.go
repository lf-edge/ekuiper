// Copyright 2025 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

type AggFuncOp struct {
	AggFields []*ast.Field
}

func (a *AggFuncOp) Apply(ctx api.StreamContext, data interface{}, _ *xsql.FunctionValuer, afv *xsql.AggregateFunctionValuer) interface{} {
	switch input := data.(type) {
	case error:
		return input
	case *xsql.Tuple:
		results := make(map[string]any)
		for _, aggField := range a.AggFields {
			afv.SetData(input)
			ve := &xsql.ValuerEval{Valuer: afv}
			v := ve.Eval(aggField.Expr)
			if err, ok := v.(error); ok {
				return err
			}
			results[aggField.Name] = v
		}
		for k, v := range results {
			input.Set(k, v)
		}
		return input
	case xsql.Collection:
		results := make(map[string]any)
		for _, aggField := range a.AggFields {
			input.GroupRange(func(i int, aggRow xsql.CollectionRow) (bool, error) {
				afv.SetData(aggRow)
				ve := &xsql.ValuerEval{Valuer: afv}
				v := ve.Eval(aggField.Expr)
				if err, ok := v.(error); ok {
					return false, err
				}
				results[aggField.Name] = v
				return true, nil
			})
		}
		input.RangeSet(func(i int, r xsql.Row) (bool, error) {
			for k, v := range results {
				r.Set(k, v)
			}
			return true, nil
		})
		return input
	default:
		return data
	}
}
