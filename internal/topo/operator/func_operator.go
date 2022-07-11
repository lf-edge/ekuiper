// Copyright 2022 EMQ Technologies Co., Ltd.
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

type FuncOp struct {
	IsAgg    bool
	CallExpr *ast.Call
	Name     string
}

func (p *FuncOp) Apply(ctx api.StreamContext, data interface{}, fv *xsql.FunctionValuer, afv *xsql.AggregateFunctionValuer) interface{} {
	ctx.GetLogger().Debugf("FuncOp receive: %s", data)
	switch input := data.(type) {
	case error:
		return input
	case xsql.TupleRow:
		ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(input, fv)}
		result := ve.Eval(p.CallExpr)
		if e, ok := result.(error); ok {
			return e
		}
		input.Set(p.Name, result)
	case xsql.SingleCollection:
		var err error
		if p.IsAgg {
			input.SetIsAgg(true)
			err = input.GroupRange(func(_ int, aggRow xsql.CollectionRow) (bool, error) {
				afv.SetData(aggRow)
				ve := &xsql.ValuerEval{Valuer: xsql.MultiAggregateValuer(aggRow, fv, aggRow, fv, afv, &xsql.WildcardValuer{Data: aggRow})}
				result := ve.Eval(p.CallExpr)
				if e, ok := result.(error); ok {
					return false, e
				}
				aggRow.Set(p.Name, result)
				return true, nil
			})
		} else {
			err = input.RangeSet(func(_ int, row xsql.Row) (bool, error) {
				ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(row, &xsql.WindowRangeValuer{WindowRange: input.GetWindowRange()}, fv, &xsql.WildcardValuer{Data: row})}
				result := ve.Eval(p.CallExpr)
				if e, ok := result.(error); ok {
					return false, e
				}
				row.Set(p.Name, result)
				return true, nil
			})
		}
		if err != nil {
			return err
		}
	case xsql.GroupedCollection: // The order is important, because single collection usually is also a groupedCollection
		if !p.IsAgg {
			return fmt.Errorf("FuncOp: GroupedCollection is not supported for non-aggregate function")
		}
		err := input.GroupRange(func(_ int, aggRow xsql.CollectionRow) (bool, error) {
			afv.SetData(aggRow)
			ve := &xsql.ValuerEval{Valuer: xsql.MultiAggregateValuer(aggRow, fv, aggRow, fv, afv, &xsql.WildcardValuer{Data: aggRow})}
			result := ve.Eval(p.CallExpr)
			if e, ok := result.(error); ok {
				return false, e
			}
			aggRow.Set(p.Name, result)
			return true, nil
		})
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("run func error: invalid input %[1]T(%[1]v)", input)
	}
	return data
}
