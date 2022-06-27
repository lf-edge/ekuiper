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
	"github.com/lf-edge/ekuiper/internal/binder/function"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/message"
)

type ProjectOp struct {
	Fields      ast.Fields
	IsAggregate bool
	SendMeta    bool
}

// Apply
//  input: *xsql.Tuple| xsql.Collection
// output: []map[string]interface{}
func (pp *ProjectOp) Apply(ctx api.StreamContext, data interface{}, fv *xsql.FunctionValuer, afv *xsql.AggregateFunctionValuer) interface{} {
	log := ctx.GetLogger()
	log.Debugf("project plan receive %s", data)
	var results []map[string]interface{}
	switch input := data.(type) {
	case error:
		return input
	case *xsql.Tuple:
		ve := pp.getVE(input, input, nil, fv, afv)
		if r, err := project(pp.Fields, ve); err != nil {
			return fmt.Errorf("run Select error: %s", err)
		} else {
			if pp.SendMeta && input.Metadata != nil {
				r[message.MetaKey] = input.Metadata
			}
			results = append(results, r)
		}
	case xsql.SingleCollection:
		var err error
		if pp.IsAggregate {
			err = input.GroupRange(func(_ int, aggRow xsql.CollectionRow) (bool, error) {
				ve := pp.getVE(aggRow, aggRow, input.GetWindowRange(), fv, afv)
				if r, err := project(pp.Fields, ve); err != nil {
					return false, fmt.Errorf("run Select error: %s", err)
				} else {
					results = append(results, r)
				}
				return true, nil
			})
		} else {
			err = input.Range(func(_ int, row xsql.TupleRow) (bool, error) {
				aggData, ok := input.(xsql.AggregateData)
				if !ok {
					return false, fmt.Errorf("unexpected type, cannot find aggregate data")
				}
				ve := pp.getVE(row, aggData, input.GetWindowRange(), fv, afv)
				if r, err := project(pp.Fields, ve); err != nil {
					return false, fmt.Errorf("run Select error: %s", err)
				} else {
					results = append(results, r)
				}
				return true, nil
			})
		}
		if err != nil {
			return err
		}
	case xsql.GroupedCollection: // The order is important, because single collection usually is also a groupedCollection
		err := input.GroupRange(func(_ int, aggRow xsql.CollectionRow) (bool, error) {
			ve := pp.getVE(aggRow, aggRow, input.GetWindowRange(), fv, afv)
			if r, err := project(pp.Fields, ve); err != nil {
				return false, fmt.Errorf("run Select error: %s", err)
			} else {
				results = append(results, r)
			}
			return true, nil
		})
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("run Select error: invalid input %[1]T(%[1]v)", input)
	}

	return results
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

func project(fs ast.Fields, ve *xsql.ValuerEval) (map[string]interface{}, error) {
	result := make(map[string]interface{}, len(fs))
	for _, f := range fs {
		vi := ve.Eval(f.Expr)
		if e, ok := vi.(error); ok {
			return nil, e
		}
		if _, ok := f.Expr.(*ast.Wildcard); ok || f.Name == "*" {
			switch val := vi.(type) {
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
			if vi != nil {
				switch vt := vi.(type) {
				case function.ResultCols:
					for k, v := range vt {
						if _, ok := result[k]; !ok {
							result[k] = v
						}
					}
				default:
					n := assignName(f.Name, f.AName)
					result[n] = vt
				}
			}
		}
	}
	return result, nil
}

func assignName(name, alias string) string {
	if alias != "" {
		return alias
	}
	return name
}
