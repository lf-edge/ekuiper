// Copyright 2021-2024 EMQ Technologies Co., Ltd.
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
)

type HavingOp struct {
	Condition  ast.Expr
	StateFuncs []*ast.Call
	IsIncAgg   bool
}

func (p *HavingOp) Apply(ctx api.StreamContext, data interface{}, fv *xsql.FunctionValuer, afv *xsql.AggregateFunctionValuer) interface{} {
	log := ctx.GetLogger()
	log.Debugf("having plan receive %v", data)
	switch input := data.(type) {
	case error:
		return input
	case xsql.Collection:
		var groups []int
		if !p.IsIncAgg {
			err := input.GroupRange(func(i int, aggRow xsql.CollectionRow) (bool, error) {
				afv.SetData(aggRow)
				ve := &xsql.ValuerEval{Valuer: xsql.MultiAggregateValuer(aggRow, fv, aggRow, fv, afv, &xsql.WildcardValuer{Data: aggRow})}
				result := ve.Eval(p.Condition)
				switch val := result.(type) {
				case error:
					return false, fmt.Errorf("run Having error: %s", val)
				case bool:
					if val {
						groups = append(groups, i)
					}
					return true, nil
				default:
					return false, fmt.Errorf("run Having error: invalid condition that returns non-bool value %[1]T(%[1]v)", val)
				}
			})
			if err != nil {
				return err
			}
			if len(groups) > 0 {
				// update trigger
				ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(fv)}
				for _, f := range p.StateFuncs {
					_ = ve.Eval(f)
				}
				switch gi := input.(type) {
				case *xsql.GroupedTuplesSet:
					return gi.Filter(groups)
				default:
					return gi
				}
			}
		} else {
			err := input.RangeSet(func(i int, row xsql.Row) (bool, error) {
				ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(fv, row, &xsql.WildcardValuer{Data: row})}
				result := ve.Eval(p.Condition)
				switch val := result.(type) {
				case error:
					return false, fmt.Errorf("run Having error: %s", val)
				case bool:
					if val {
						groups = append(groups, i)
					}
					return true, nil
				default:
					return false, fmt.Errorf("run Having error: invalid condition that returns non-bool value %[1]T(%[1]v)", val)
				}
			})
			if err != nil {
				return err
			}
			if len(groups) > 0 {
				ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(fv)}
				for _, f := range p.StateFuncs {
					_ = ve.Eval(f)
				}
				return input.Filter(groups)
			}
		}
	default:
		return fmt.Errorf("run Having error: invalid input %[1]T(%[1]v)", input)
	}
	return nil
}
