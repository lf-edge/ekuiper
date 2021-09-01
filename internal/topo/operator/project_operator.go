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
	"encoding/json"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/message"
	"strings"
)

type ProjectOp struct {
	Fields      ast.Fields
	IsAggregate bool
	SendMeta    bool
}

/**
 *  input: *xsql.Tuple from preprocessor or filterOp | xsql.WindowTuplesSet from windowOp or filterOp | xsql.JoinTupleSets from joinOp or filterOp
 *  output: []map[string]interface{}
 */
func (pp *ProjectOp) Apply(ctx api.StreamContext, data interface{}, fv *xsql.FunctionValuer, afv *xsql.AggregateFunctionValuer) interface{} {
	log := ctx.GetLogger()
	log.Debugf("project plan receive %s", data)
	var results []map[string]interface{}
	switch input := data.(type) {
	case error:
		return input
	case *xsql.Tuple:
		ve := pp.getVE(input, input, fv, afv)
		if r, err := project(pp.Fields, ve); err != nil {
			return fmt.Errorf("run Select error: %s", err)
		} else {
			if pp.SendMeta && input.Metadata != nil {
				r[message.MetaKey] = input.Metadata
			}
			results = append(results, r)
		}
	case xsql.WindowTuplesSet:
		if len(input.Content) != 1 {
			return fmt.Errorf("run Select error: the input WindowTuplesSet with multiple tuples cannot be evaluated)")
		}
		ms := input.Content[0].Tuples
		for _, v := range ms {
			ve := pp.getVE(&v, input, fv, afv)
			if r, err := project(pp.Fields, ve); err != nil {
				return fmt.Errorf("run Select error: %s", err)
			} else {
				results = append(results, r)
			}
			if pp.IsAggregate {
				break
			}
		}
	case *xsql.JoinTupleSets:
		ms := input.Content
		for _, v := range ms {
			ve := pp.getVE(&v, input, fv, afv)
			if r, err := project(pp.Fields, ve); err != nil {
				return err
			} else {
				results = append(results, r)
			}
			if pp.IsAggregate {
				break
			}
		}
	case xsql.GroupedTuplesSet:
		for _, v := range input {
			ve := pp.getVE(v.Content[0], v, fv, afv)
			if r, err := project(pp.Fields, ve); err != nil {
				return fmt.Errorf("run Select error: %s", err)
			} else {
				results = append(results, r)
			}
		}
	default:
		return fmt.Errorf("run Select error: invalid input %[1]T(%[1]v)", input)
	}

	if ret, err := json.Marshal(results); err == nil {
		return ret
	} else {
		return fmt.Errorf("run Select error: %v", err)
	}
}

func (pp *ProjectOp) getVE(tuple xsql.DataValuer, agg xsql.AggregateData, fv *xsql.FunctionValuer, afv *xsql.AggregateFunctionValuer) *xsql.ValuerEval {
	afv.SetData(agg)
	if pp.IsAggregate {
		return &xsql.ValuerEval{Valuer: xsql.MultiAggregateValuer(agg, fv, tuple, fv, afv, &xsql.WildcardValuer{Data: tuple})}
	} else {
		var wr *xsql.WindowRange
		switch input := agg.(type) {
		case xsql.WindowTuplesSet:
			wr = input.WindowRange
		case *xsql.JoinTupleSets:
			wr = input.WindowRange
		}
		if wr != nil {
			return &xsql.ValuerEval{Valuer: xsql.MultiValuer(tuple, &xsql.WindowRangeValuer{WindowRange: wr}, fv, &xsql.WildcardValuer{Data: tuple})}
		}
		return &xsql.ValuerEval{Valuer: xsql.MultiValuer(tuple, fv, &xsql.WildcardValuer{Data: tuple})}
	}
}

func project(fs ast.Fields, ve *xsql.ValuerEval) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	for _, f := range fs {
		v := ve.Eval(f.Expr)
		if e, ok := v.(error); ok {
			return nil, e
		}
		if _, ok := f.Expr.(*ast.Wildcard); ok || f.Name == "*" {
			switch val := v.(type) {
			case map[string]interface{}:
				for k, v := range val {
					if _, ok := result[k]; !ok {
						result[k] = v
					}
				}
			case xsql.Message:
				for k, v := range val {
					if _, ok := result[k]; !ok {
						result[k] = v
					}
				}
			default:
				return nil, fmt.Errorf("wildcarder does not return map")
			}
		} else {
			if v != nil {
				n := assignName(f.Name, f.AName)
				if _, ok := result[n]; !ok {
					result[n] = v
				}
			}
		}
	}
	return result, nil
}

func assignName(name, alias string) string {
	if result := strings.Trim(alias, " "); result != "" {
		return result
	}

	if result := strings.Trim(name, " "); result != "" {
		return result
	}
	return ""
}
