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

package xsql

import (
	"fmt"
	"sort"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
)

// MultiSorter implements the Sort interface, sorting the changes within.
type MultiSorter struct {
	Ctx api.StreamContext
	SortingData
	fields    ast.SortFields
	valuer    *FunctionValuer
	aggValuer *AggregateFunctionValuer
	values    []map[string]interface{}
}

func (ms *MultiSorter) GetTracerCtx() api.StreamContext {
	return ms.Ctx
}

func (ms *MultiSorter) SetTracerCtx(ctx api.StreamContext) {
	ms.Ctx = ctx
}

// OrderedBy returns a Sorter that sorts using the less functions, in order.
// Call its Sort method to sort the data.
func OrderedBy(fields ast.SortFields, fv *FunctionValuer, afv *AggregateFunctionValuer) *MultiSorter {
	return &MultiSorter{
		fields:    fields,
		valuer:    fv,
		aggValuer: afv,
	}
}

// Less is part of sort.Interface. It is implemented by looping along the
// less functions until it finds a comparison that discriminates between
// the two items (one is less than the other). Note that it can call the
// less functions twice per call. We could change the functions to return
// -1, 0, 1 and reduce the number of calls for greater efficiency: an
// exercise for the reader.
func (ms *MultiSorter) Less(i, j int) bool {
	p, q := ms.values[i], ms.values[j]
	v := &ValuerEval{Valuer: MultiValuer(ms.valuer)}
	for _, field := range ms.fields {
		n := field.Uname
		vp, _ := p[n]
		vq, _ := q[n]
		if vp == nil && vq != nil {
			return false
		} else if vp != nil && vq == nil {
			return true
		} else if vp == nil && vq == nil {
			return false
		}
		switch {
		case v.SimpleDataEval(vp, vq, ast.LT):
			return field.Ascending
		case v.SimpleDataEval(vq, vp, ast.LT):
			return !field.Ascending
		}
	}
	return false
}

func (ms *MultiSorter) Swap(i, j int) {
	ms.values[i], ms.values[j] = ms.values[j], ms.values[i]
	ms.SortingData.Swap(i, j)
}

// Sort sorts the argument slice according to the fewer functions passed to OrderedBy.
func (ms *MultiSorter) Sort(data SortingData) error {
	ms.SortingData = data
	types := make([]string, len(ms.fields))
	ms.values = make([]map[string]interface{}, data.Len())
	switch input := data.(type) {
	case error:
		return input
	case Collection:
		err := input.RangeSet(func(i int, row Row) (bool, error) {
			var vep *ValuerEval
			if aggRow, ok := row.(AggregateData); ok {
				ms.aggValuer.SetData(aggRow)
				vep = &ValuerEval{Valuer: MultiAggregateValuer(aggRow, ms.valuer, row, ms.aggValuer, &WildcardValuer{Data: row})}
			} else {
				vep = &ValuerEval{Valuer: MultiValuer(ms.valuer, row, ms.valuer, &WildcardValuer{Data: row})}
			}
			ms.values[i] = make(map[string]interface{})

			for j, field := range ms.fields {
				vp := vep.Eval(field.FieldExpr)
				if types[j] == "" && vp != nil {
					types[j] = fmt.Sprintf("%T", vp)
				}
				if err := validate(types[j], vp); err != nil {
					return false, err
				} else {
					ms.values[i][field.Uname] = vp
				}
			}
			return true, nil
		})
		if err != nil {
			return err
		}
	default:
		return nil
	}
	sort.Sort(ms)
	return nil
}

func validate(t string, v interface{}) error {
	if v == nil || t == "" {
		return nil
	}
	vt := fmt.Sprintf("%T", v)
	switch t {
	case "int", "int64", "float64", "uint64":
		if vt == "int" || vt == "int64" || vt == "float64" || vt == "uint64" {
			return nil
		} else {
			return fmt.Errorf("incompatible types for comparison: %s and %s", t, vt)
		}
	case "bool":
		if vt == "bool" {
			return nil
		} else {
			return fmt.Errorf("incompatible types for comparison: %s and %s", t, vt)
		}
	case "string":
		if vt == "string" {
			return nil
		} else {
			return fmt.Errorf("incompatible types for comparison: %s and %s", t, vt)
		}
	case "time.Time":
		_, err := cast.InterfaceToTime(v, "")
		if err != nil {
			return fmt.Errorf("incompatible types for comparison: %s and %s", t, vt)
		} else {
			return nil
		}
	default:
		return fmt.Errorf("incompatible types for comparison: %s and %s", t, vt)
	}
}
