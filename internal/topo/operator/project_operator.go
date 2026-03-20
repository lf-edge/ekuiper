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

	"github.com/lf-edge/ekuiper/v2/internal/binder/function"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
)

type ProjectOp struct {
	ColNames         [][]string // list of [col, table]
	ExceptNames      []string   // list of except name
	AllWildcard      bool
	WildcardEmitters map[string]bool
	AliasFields      ast.Fields
	ExprFields       ast.Fields
	Fields           ast.Fields
	// the length of fields exclude invisible
	FieldLen    int
	IsAggregate bool // Whether the project is used in an aggregate context. This is set by planner by analyzing the SQL query
	EnableLimit bool
	LimitCount  int

	SendMeta bool
	SendNil  bool

	kvs   []interface{}
	alias []interface{}

	ve  *xsql.ValuerEval
	wv  *xsql.WildcardValuer
	wrv *xsql.WindowRangeValuer
	mvs xsql.MultiValuerList
	mav *xsql.AggregateMultiValuer

	// compiledExprs caches pre-compiled accessors for ExprFields.
	// For simple FieldRef fields, isDirect=true and we skip the AST walk.
	compiledExprs []compiledField
	aliasIndices  []int
	fieldIndices  []int
}

// compiledField caches whether an ExprField is a simple direct lookup
// or requires the full ValuerEval walk.
type compiledField struct {
	name     string   // output field name
	isDirect bool     // true: use dirKey/dirTable directly on row
	dirKey   string   // source key for direct access
	dirTable string   // source table for direct access (empty = default)
	expr     ast.Expr // non-nil when isDirect == false (complex expr)
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
		return []xsql.Row{}
	}
	switch input := data.(type) {
	case error:
		return input
	case xsql.Row:
		ve := pp.getRowVE(input, nil, fv, afv)
		if err := pp.project(input, ve); err != nil {
			return fmt.Errorf("run Select error: %s", err)
		} else {
			if pp.SendMeta {
				if md, ok := input.(xsql.MetaData); ok {
					metadata := md.MetaData()
					if metadata != nil {
						input.Set(message.MetaKey, md.MetaData())
					}
				}
			}
		}
	case xsql.Collection:
		var err error
		if pp.IsAggregate {
			input.SetIsAgg(true)
			err = input.GroupRange(func(i int, aggRow xsql.CollectionRow) (bool, error) {
				if pp.EnableLimit && pp.LimitCount > 0 && i >= pp.LimitCount {
					return false, nil
				}
				ve := pp.getVE(aggRow, aggRow, input.GetWindowRange(), fv, afv)
				if err := pp.project(aggRow, ve); err != nil {
					return false, fmt.Errorf("run Select error: %s", err)
				}
				return true, nil
			})
		} else {
			err = input.RangeSet(func(i int, row xsql.Row) (bool, error) {
				if pp.EnableLimit && pp.LimitCount > 0 && i >= pp.LimitCount {
					return false, nil
				}
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
	default:
		return fmt.Errorf("run Select error: invalid input %[1]T(%[1]v)", input)
	}
	return data
}

func (pp *ProjectOp) getVE(tuple xsql.RawRow, agg xsql.AggregateData, wr *xsql.WindowRange, fv *xsql.FunctionValuer, afv *xsql.AggregateFunctionValuer) *xsql.ValuerEval {
	afv.SetData(agg)
	// Lazy init
	if pp.ve == nil {
		pp.ve = &xsql.ValuerEval{}
		pp.wv = &xsql.WildcardValuer{}
		if pp.IsAggregate {
			pp.mvs = make(xsql.MultiValuerList, 4)
			pp.mav = &xsql.AggregateMultiValuer{
				MultiValuerList: pp.mvs,
			}
		} else {
			if wr != nil {
				pp.mvs = make(xsql.MultiValuerList, 4)
				pp.wrv = &xsql.WindowRangeValuer{}
			} else {
				pp.mvs = make(xsql.MultiValuerList, 3)
			}
		}
		// Pre-allocate buffers based on field counts
		fullLen := len(pp.Fields) + len(pp.AliasFields)
		if cap(pp.kvs) < fullLen*2 {
			pp.kvs = make([]interface{}, 0, fullLen*2)
		}
		if cap(pp.alias) < len(pp.AliasFields)*2 {
			pp.alias = make([]interface{}, 0, len(pp.AliasFields)*2)
		}
		// Pre-compile ExprField accessors: for simple FieldRef (non-alias, non-indexed)
		// we can call row.Value directly, skipping the full Eval dispatch.
		pp.compiledExprs = make([]compiledField, 0, len(pp.ExprFields))
		for _, f := range pp.ExprFields {
			if f.Invisible {
				continue
			}
			cf := compiledField{name: f.Name, expr: f.Expr}
			if fr, ok := f.Expr.(*ast.FieldRef); ok && !fr.IsAlias() && !fr.HasIndex {
				cf.isDirect = true
				cf.dirKey = fr.Name
				if fr.StreamName != ast.DefaultStream {
					cf.dirTable = string(fr.StreamName)
				}
			}
			pp.compiledExprs = append(pp.compiledExprs, cf)
		}
		pp.aliasIndices = make([]int, len(pp.AliasFields))
		for i, f := range pp.AliasFields {
			pp.aliasIndices[i] = getExprIndex(f.Expr)
		}
		pp.fieldIndices = make([]int, len(pp.Fields))
		for i, f := range pp.Fields {
			pp.fieldIndices[i] = getExprIndex(f.Expr)
		}
	}

	pp.wv.Data = tuple
	if pp.IsAggregate {
		pp.mvs[0] = tuple
		pp.mvs[1] = fv
		pp.mvs[2] = afv
		pp.mvs[3] = pp.wv
		pp.mav.Data = agg
		pp.mav.SingleCallValuer = fv
		pp.ve.Valuer = pp.mav
	} else {
		if wr != nil {
			pp.wrv.WindowRange = wr
			pp.mvs[0] = tuple
			pp.mvs[1] = pp.wrv
			pp.mvs[2] = fv
			pp.mvs[3] = pp.wv
		} else {
			pp.mvs[0] = tuple
			pp.mvs[1] = fv
			pp.mvs[2] = pp.wv
		}
		pp.ve.Valuer = pp.mvs
	}
	return pp.ve
}

func (pp *ProjectOp) getRowVE(tuple xsql.Row, wr *xsql.WindowRange, fv *xsql.FunctionValuer, afv *xsql.AggregateFunctionValuer) *xsql.ValuerEval {
	if ag, ok := tuple.(xsql.AggregateData); ok {
		return pp.getVE(tuple, ag, wr, fv, afv)
	} else {
		return pp.getVE(tuple, nil, wr, fv, afv)
	}
}

func (pp *ProjectOp) project(row xsql.RawRow, ve *xsql.ValuerEval) error {
	switch rt := row.(type) {
	case *xsql.SliceTuple:
		for i, f := range pp.AliasFields {
			vi := ve.Eval(f.Expr)
			if e, ok := vi.(error); ok {
				return fmt.Errorf("expr: %s meet error, err:%v", f.Expr.String(), e)
			}
			if pp.SendNil && vi == nil {
				vi = cast.TNil
			}
			index := pp.aliasIndices[i]
			if index >= 0 {
				rt.SetByIndex(index, vi)
			}
		}
		for i, f := range pp.Fields {
			if f.AName == "" {
				vi := ve.Eval(f.Expr)
				if e, ok := vi.(error); ok {
					return fmt.Errorf("expr: %s meet error, err:%v", f.Expr.String(), e)
				}
				if pp.SendNil && vi == nil {
					vi = cast.TNil
				}
				index := pp.fieldIndices[i]
				if index >= 0 {
					rt.SetByIndex(index, vi)
				}
			}
		}
		rt.Compact(pp.FieldLen)
	default:
		// Calculate all fields then pick the needed ones
		// To make sure all calculations are run with the same context (e.g. alias values)
		// Do not set value during calculations
		pp.kvs = pp.kvs[:0]
		pp.alias = pp.alias[:0]

		for _, cf := range pp.compiledExprs {
			var vi interface{}
			if cf.isDirect {
				// Fast path: direct map lookup, skips AST walk
				vi, _ = row.Value(cf.dirKey, cf.dirTable)
			} else {
				vi = ve.Eval(cf.expr)
				if e, ok := vi.(error); ok {
					return fmt.Errorf("expr: %s meet error, err:%v", cf.expr.String(), e)
				}
			}
			if vi != nil {
				switch vt := vi.(type) {
				case function.ResultCols:
					for i, v := range vt.IndexValues {
						if v != nil {
							k := vt.Keys[i]
							pp.kvs = append(pp.kvs, k, v)
						}
					}
				default:
					pp.kvs = append(pp.kvs, cf.name, vi)
				}
			}
		}
		for _, f := range pp.AliasFields {
			vi := ve.Eval(f.Expr)
			if e, ok := vi.(error); ok {
				if ref, ok := f.Expr.(*ast.FieldRef); ok {
					s := ref.AliasRef.Expression.String()
					return fmt.Errorf("alias: %v expr: %v meet error, err:%v", f.AName, s, e)
				}
				return fmt.Errorf("alias: %v expr: %v meet error, err:%v", f.AName, f.Expr.String(), e)
			}
			if !f.Invisible && (vi != nil || pp.SendNil) {
				pp.alias = append(pp.alias, f.AName, vi)
			}
		}
		row.Pick(pp.AllWildcard, pp.ColNames, pp.WildcardEmitters, pp.ExceptNames, pp.SendNil)

		kvsLen := len(pp.kvs)
		for i := 0; i < kvsLen; i += 2 {
			row.Set(pp.kvs[i].(string), pp.kvs[i+1])
		}
		aliasLen := len(pp.alias)
		for i := 0; i < aliasLen; i += 2 {
			row.AppendAlias(pp.alias[i].(string), pp.alias[i+1])
		}
	}
	return nil
}

func getExprIndex(expr ast.Expr) int {
	if fr, ok := expr.(*ast.FieldRef); ok {
		return fr.Index
	}
	var targetIdx int = -1
	ast.WalkFunc(expr, func(n ast.Node) bool {
		if fr, ok := n.(*ast.FieldRef); ok {
			targetIdx = fr.Index
			return false // stop walking
		}
		return true
	})
	return targetIdx
}
