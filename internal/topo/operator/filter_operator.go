// Copyright 2021-2022 EMQ Technologies Co., Ltd.
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

// Apply
/*
 *  input: *xsql.Tuple from preprocessor | xsql.WindowTuples from windowOp | xsql.JoinTuples from joinOp
 *  output: *xsql.Tuple | xsql.WindowTuples | xsql.JoinTuples
 */
func (p *FilterOp) Apply(ctx api.StreamContext, data interface{}, fv *xsql.FunctionValuer, afv *xsql.AggregateFunctionValuer) interface{} {
	log := ctx.GetLogger()
	log.Debugf("filter plan receive %s", data)
	switch input := data.(type) {
	case error:
		return input
	case xsql.TupleRow:
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
	case xsql.SingleCollection:
		var sel []int
		err := input.Range(func(i int, r xsql.ReadonlyRow) (bool, error) {
			ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(r, fv)}
			result := ve.Eval(p.Condition)
			switch val := result.(type) {
			case error:
				return false, fmt.Errorf("run Where error: %s", val)
			case bool:
				if val {
					sel = append(sel, i)
				}
			case nil:
				break
			default:
				return false, fmt.Errorf("run Where error: invalid condition that returns non-bool value %[1]T(%[1]v)", val)
			}
			return true, nil
		})
		if err != nil {
			return err
		}
		r := input.Filter(sel)
		if r.Len() == 0 {
			return nil
		}
		return r
	default:
		return fmt.Errorf("run Where error: invalid input %[1]T(%[1]v)", input)
	}
	return nil
}
