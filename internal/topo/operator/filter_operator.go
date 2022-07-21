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

type FilterOp struct {
	Condition ast.Expr
}

/**
 *  input: *xsql.Tuple from preprocessor | xsql.WindowTuplesSet from windowOp | xsql.JoinTupleSets from joinOp
 *  output: *xsql.Tuple | xsql.WindowTuplesSet | xsql.JoinTupleSets
 */
func (p *FilterOp) Apply(ctx api.StreamContext, data interface{}, fv *xsql.FunctionValuer, afv *xsql.AggregateFunctionValuer) interface{} {
	log := ctx.GetLogger()
	log.Debugf("filter plan receive %s", data)
	switch input := data.(type) {
	case error:
		return input
	case xsql.Valuer:
		ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(input, fv)}
		result := ve.Eval(p.Condition)
		switch r := result.(type) {
		case error:
			return fmt.Errorf("run Where error: %s", r)
		case bool:
			if r {
				return input
			}
		case nil: // nil is false
			break
		default:
			return fmt.Errorf("run Where error: invalid condition that returns non-bool value %[1]T(%[1]v)", r)
		}
	case xsql.WindowTuples: // For batch table, will return the batch
		var f []xsql.Tuple
		for _, t := range input.Tuples {
			ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(&t, fv)}
			result := ve.Eval(p.Condition)
			switch val := result.(type) {
			case error:
				return fmt.Errorf("run Where error: %s", val)
			case bool:
				if val {
					f = append(f, t)
				}
			case nil:
				break
			default:
				return fmt.Errorf("run Where error: invalid condition that returns non-bool value %[1]T(%[1]v)", val)
			}
		}
		input.Tuples = f
		return input
	case xsql.WindowTuplesSet:
		if len(input.Content) != 1 {
			return fmt.Errorf("run Where error: the input WindowTuplesSet with multiple tuples cannot be evaluated")
		}
		ms := input.Content[0].Tuples
		r := ms[:0]
		for _, v := range ms {
			ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(&v, fv)}
			result := ve.Eval(p.Condition)
			switch val := result.(type) {
			case error:
				return fmt.Errorf("run Where error: %s", val)
			case bool:
				if val {
					r = append(r, v)
				}
			case nil:
				break
			default:
				return fmt.Errorf("run Where error: invalid condition that returns non-bool value %[1]T(%[1]v)", val)
			}
		}
		if len(r) > 0 {
			input.Content[0].Tuples = r
			return input
		}
	case *xsql.JoinTupleSets:
		ms := input.Content
		r := ms[:0]
		for _, v := range ms {
			ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(&v, fv)}
			result := ve.Eval(p.Condition)
			switch val := result.(type) {
			case error:
				return fmt.Errorf("run Where error: %s", val)
			case bool:
				if val {
					r = append(r, v)
				}
			case nil:
				break
			default:
				return fmt.Errorf("run Where error: invalid condition that returns non-bool value %[1]T(%[1]v)", val)
			}
		}
		input.Content = r
		if len(r) > 0 {
			return input
		}
	default:
		return fmt.Errorf("run Where error: invalid input %[1]T(%[1]v)", input)
	}
	return nil
}
