// Copyright 2021 EMQ Technologies Co., Ltd.
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

type HavingOp struct {
	Condition ast.Expr
}

func (p *HavingOp) Apply(ctx api.StreamContext, data interface{}, fv *xsql.FunctionValuer, afv *xsql.AggregateFunctionValuer) interface{} {
	log := ctx.GetLogger()
	log.Debugf("having plan receive %s", data)
	switch input := data.(type) {
	case error:
		return input
	case xsql.GroupedTuplesSet:
		r := xsql.GroupedTuplesSet{}
		for _, v := range input {
			afv.SetData(v)
			ve := &xsql.ValuerEval{Valuer: xsql.MultiAggregateValuer(v, fv, v.Content[0], fv, afv, &xsql.WildcardValuer{Data: v.Content[0]})}
			result := ve.Eval(p.Condition)
			switch val := result.(type) {
			case error:
				return fmt.Errorf("run Having error: %s", val)
			case bool:
				if val {
					r = append(r, v)
				}
			default:
				return fmt.Errorf("run Having error: invalid condition that returns non-bool value %[1]T(%[1]v)", val)
			}
		}
		if len(r) > 0 {
			return r
		}
	case xsql.WindowTuplesSet:
		if len(input.Content) != 1 {
			return fmt.Errorf("run Having error: input WindowTuplesSet with multiple tuples cannot be evaluated")
		}
		ms := input.Content[0].Tuples
		v := ms[0]
		afv.SetData(input)
		ve := &xsql.ValuerEval{Valuer: xsql.MultiAggregateValuer(input, fv, &v, fv, afv, &xsql.WildcardValuer{Data: &v})}
		result := ve.Eval(p.Condition)
		switch val := result.(type) {
		case error:
			return fmt.Errorf("run Having error: %s", val)
		case bool:
			if val {
				return input
			}
		default:
			return fmt.Errorf("run Having error: invalid condition that returns non-bool value %[1]T(%[1]v)", val)
		}
	case *xsql.JoinTupleSets:
		ms := input.Content
		r := ms[:0]
		afv.SetData(input)
		for _, v := range ms {
			ve := &xsql.ValuerEval{Valuer: xsql.MultiAggregateValuer(input, fv, &v, fv, afv, &xsql.WildcardValuer{Data: &v})}
			result := ve.Eval(p.Condition)
			switch val := result.(type) {
			case error:
				return fmt.Errorf("run Having error: %s", val)
			case bool:
				if val {
					r = append(r, v)
				}
			default:
				return fmt.Errorf("run Having error: invalid condition that returns non-bool value %[1]T(%[1]v)", val)
			}
		}
		input.Content = r
		if len(r) > 0 {
			return input
		}
	default:
		return fmt.Errorf("run Having error: invalid input %[1]T(%[1]v)", input)
	}
	return nil
}
