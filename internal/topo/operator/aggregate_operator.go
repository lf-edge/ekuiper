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

type AggregateOp struct {
	Dimensions ast.Dimensions
}

/**
 *  input: *xsql.Tuple from preprocessor | xsql.WindowTuplesSet from windowOp | xsql.JoinTupleSets from joinOp
 *  output: xsql.GroupedTuplesSet
 */
func (p *AggregateOp) Apply(ctx api.StreamContext, data interface{}, fv *xsql.FunctionValuer, _ *xsql.AggregateFunctionValuer) interface{} {
	log := ctx.GetLogger()
	log.Debugf("aggregate plan receive %s", data)
	grouped := data
	var wr *xsql.WindowRange
	if p.Dimensions != nil {
		var ms []xsql.DataValuer
		switch input := data.(type) {
		case error:
			return input
		case xsql.DataValuer:
			ms = append(ms, input)
		case xsql.WindowTuplesSet:
			if len(input.Content) != 1 {
				return fmt.Errorf("run Group By error: the input WindowTuplesSet with multiple tuples cannot be evaluated")
			}
			ms = make([]xsql.DataValuer, len(input.Content[0].Tuples))
			for i, m := range input.Content[0].Tuples {
				//this is needed or it will always point to the last
				t := m
				ms[i] = &t
			}
			wr = input.WindowRange
		case *xsql.JoinTupleSets:
			ms = make([]xsql.DataValuer, len(input.Content))
			for i, m := range input.Content {
				t := m
				ms[i] = &t
			}
			wr = input.WindowRange
		default:
			return fmt.Errorf("run Group By error: invalid input %[1]T(%[1]v)", input)
		}

		result := make(map[string]*xsql.GroupedTuples)
		for _, m := range ms {
			var name string
			ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(m, &xsql.WindowRangeValuer{WindowRange: wr}, fv)}
			for _, d := range p.Dimensions {
				r := ve.Eval(d.Expr)
				if _, ok := r.(error); ok {
					return fmt.Errorf("run Group By error: %s", r)
				} else {
					name += fmt.Sprintf("%v,", r)
				}
			}
			if ts, ok := result[name]; !ok {
				result[name] = &xsql.GroupedTuples{Content: []xsql.DataValuer{m}, WindowRange: wr}
			} else {
				ts.Content = append(ts.Content, m)
			}
		}
		if len(result) > 0 {
			g := make([]xsql.GroupedTuples, 0, len(result))
			for _, v := range result {
				g = append(g, *v)
			}
			grouped = xsql.GroupedTuplesSet(g)
		} else {
			grouped = nil
		}
	}
	return grouped
}
