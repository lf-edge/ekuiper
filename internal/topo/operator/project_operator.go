// Copyright 2022-2023 EMQ Technologies Co., Ltd.
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

	"github.com/lf-edge/ekuiper/internal/binder/function"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/message"
)

type ProjectOp struct {
	ColNames         [][]string // list of [col, table]
	AliasNames       []string   // list of alias name
	ExprNames        []string   // list of expr name
	ExceptNames      []string   // list of except name
	WindowFuncNames  map[string]struct{}
	AllWildcard      bool
	WildcardEmitters map[string]bool
	AliasFields      ast.Fields
	ExprFields       ast.Fields
	IsAggregate      bool
	EnableLimit      bool
	LimitCount       int

	SendMeta bool

	kvs   []interface{}
	alias []interface{}
}

// Apply
//
//	input: *xsql.Tuple| xsql.Collection
//
// output: []map[string]interface{}
func (pp *ProjectOp) Apply(ctx api.StreamContext, data interface{}, fv *xsql.FunctionValuer, afv *xsql.AggregateFunctionValuer) interface{} {
	log := ctx.GetLogger()
	log.Debugf("project plan receive %v", data)
	if pp.LimitCount == 0 && pp.EnableLimit {
		return []xsql.TupleRow{}
	}
	switch input := data.(type) {
	case error:
		return input
	case *xsql.Tuple:
		ve := pp.getVE(input, input, nil, fv, afv)
		if err := pp.project(input, ve); err != nil {
			return fmt.Errorf("run Select error: %s", err)
		} else {
			if pp.SendMeta && input.Metadata != nil {
				input.Set(message.MetaKey, input.Metadata)
			}
		}
	case xsql.SingleCollection:
		var err error
		if pp.IsAggregate {
			input.SetIsAgg(true)
			err = input.GroupRange(func(_ int, aggRow xsql.CollectionRow) (bool, error) {
				ve := pp.getVE(aggRow, aggRow, input.GetWindowRange(), fv, afv)
				if err := pp.project(aggRow, ve); err != nil {
					return false, fmt.Errorf("run Select error: %s", err)
				}
				return true, nil
			})
			if pp.EnableLimit && pp.LimitCount > 0 && input.Len() > pp.LimitCount {
				var sel []int
				sel = make([]int, pp.LimitCount, pp.LimitCount)
				for i := 0; i < pp.LimitCount; i++ {
					sel[i] = i
				}
				input = input.Filter(sel).(xsql.SingleCollection)
			}
		} else {
			if pp.EnableLimit && pp.LimitCount > 0 && input.Len() > pp.LimitCount {
				var sel []int
				sel = make([]int, pp.LimitCount, pp.LimitCount)
				for i := 0; i < pp.LimitCount; i++ {
					sel[i] = i
				}
				input = input.Filter(sel).(xsql.SingleCollection)
			}
			err = input.RangeSet(func(_ int, row xsql.Row) (bool, error) {
				aggData, ok := input.(xsql.AggregateData)
				if !ok {
					return false, fmt.Errorf("unexpected type, cannot find aggregate data")
				}
				ve := pp.getVE(row, aggData, input.GetWindowRange(), fv, afv)
				if err := pp.project(row, ve); err != nil {
					return false, fmt.Errorf("run Select error: %s", err)
				}
				return true, nil
			})
		}
		if err != nil {
			return err
		}
	case xsql.GroupedCollection: // The order is important, because single collection usually is also a groupedCollection
		if pp.EnableLimit && pp.LimitCount > 0 && input.Len() > pp.LimitCount {
			var sel []int
			sel = make([]int, pp.LimitCount, pp.LimitCount)
			for i := 0; i < pp.LimitCount; i++ {
				sel[i] = i
			}
			input = input.Filter(sel).(xsql.GroupedCollection)
		}
		err := input.GroupRange(func(_ int, aggRow xsql.CollectionRow) (bool, error) {
			ve := pp.getVE(aggRow, aggRow, input.GetWindowRange(), fv, afv)
			if err := pp.project(aggRow, ve); err != nil {
				return false, fmt.Errorf("run Select error: %s", err)
			}
			return true, nil
		})
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("run Select error: invalid input %[1]T(%[1]v)", input)
	}
	return data
}

func (pp *ProjectOp) getVE(tuple xsql.Row, agg xsql.AggregateData, wr *xsql.WindowRange, fv *xsql.FunctionValuer, afv *xsql.AggregateFunctionValuer) *xsql.ValuerEval {
	afv.SetData(agg)
	if pp.IsAggregate {
		return &xsql.ValuerEval{Valuer: xsql.MultiAggregateValuer(agg, fv, tuple, fv, afv, &xsql.WildcardValuer{Data: tuple})}
	} else {
		if wr != nil {
			return &xsql.ValuerEval{Valuer: xsql.MultiValuer(tuple, &xsql.WindowRangeValuer{WindowRange: wr}, fv, &xsql.WildcardValuer{Data: tuple})}
		}
		return &xsql.ValuerEval{Valuer: xsql.MultiValuer(tuple, fv, &xsql.WildcardValuer{Data: tuple})}
	}
}

func (pp *ProjectOp) project(row xsql.Row, ve *xsql.ValuerEval) error {
	// Calculate all fields then pick the needed ones
	// To make sure all calculations are run with the same context (e.g. alias values)
	// Do not set value during calculations

	for _, f := range pp.ExprFields {
		if _, ok := pp.WindowFuncNames[f.Name]; ok {
			vi, _ := row.Value(f.Name, "")
			pp.kvs = append(pp.kvs, f.Name, vi)
			continue
		}
		vi := ve.Eval(f.Expr)
		if e, ok := vi.(error); ok {
			return fmt.Errorf("expr: %s meet error, err:%v", f.Expr.String(), e)
		}
		if vi != nil {
			switch vt := vi.(type) {
			case function.ResultCols:
				for k, v := range vt {
					pp.kvs = append(pp.kvs, k, v)
				}
			default:
				pp.kvs = append(pp.kvs, f.Name, vi)
			}
		}
	}
	for _, f := range pp.AliasFields {
		if _, ok := pp.WindowFuncNames[f.AName]; ok {
			vi, _ := row.Value(f.AName, "")
			pp.kvs = append(pp.kvs, f.AName, vi)
			continue
		}
		vi := ve.Eval(f.Expr)
		if e, ok := vi.(error); ok {
			if ref, ok := f.Expr.(*ast.FieldRef); ok {
				s := ref.AliasRef.Expression.String()
				return fmt.Errorf("alias: %v expr: %v meet error, err:%v", f.AName, s, e)
			}
			return fmt.Errorf("alias: %v expr: %v meet error, err:%v", f.AName, f.Expr.String(), e)
		}
		if vi != nil {
			pp.alias = append(pp.alias, f.AName, vi)
		}
	}
	row.Pick(pp.AllWildcard, pp.ColNames, pp.WildcardEmitters, pp.ExceptNames)
	for i := 0; i < len(pp.kvs); i += 2 {
		row.Set(pp.kvs[i].(string), pp.kvs[i+1])
	}
	pp.kvs = pp.kvs[:0]
	for i := 0; i < len(pp.alias); i += 2 {
		row.AppendAlias(pp.alias[i].(string), pp.alias[i+1])
	}
	pp.alias = pp.alias[:0]
	return nil
}
