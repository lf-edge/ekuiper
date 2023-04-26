// Copyright 2023 EMQ Technologies Co., Ltd.
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
)

type ProjectSetOperator struct {
	SrfMapping map[string]struct{}
}

func (ps *ProjectSetOperator) Apply(_ api.StreamContext, data interface{}, _ *xsql.FunctionValuer, _ *xsql.AggregateFunctionValuer) interface{} {
	// for now we only support 1 srf function in the field
	srfName := ""
	for k := range ps.SrfMapping {
		srfName = k
		break
	}
	r := make([]interface{}, 0)
	switch input := data.(type) {
	case error:
		return input
	case xsql.TupleRow:
		newTuples, err := ps.handleSRFRow(srfName, input)
		if err != nil {
			return err
		}
		for _, tuple := range newTuples {
			r = append(r, tuple)
		}
		return r
	case xsql.Collection:
		collections, err := input.RangeProjectSet(func(_ int, r xsql.CloneAbleRow) ([]xsql.CloneAbleRow, error) {
			return ps.handleSRFRow(srfName, r)
		})
		if err != nil {
			return err
		}
		for _, c := range collections {
			r = append(r, c)
		}
		return r
	default:
		return fmt.Errorf("run Select error: invalid input %[1]T(%[1]v)", input)
	}
}

func (ps *ProjectSetOperator) handleSRFRow(srfName string, row xsql.CloneAbleRow) ([]xsql.CloneAbleRow, error) {
	newData := make([]xsql.CloneAbleRow, 0)
	aValue, ok := row.Value(srfName, "")
	if !ok {
		return nil, fmt.Errorf("can't find the result from the %v function", srfName)
	}
	aValues, ok := aValue.([]interface{})
	if !ok {
		return nil, fmt.Errorf("the argument for the %v function should be array", srfName)
	}
	for _, v := range aValues {
		newTupleRow := row.Clone()
		// clear original column value
		newTupleRow.Del(srfName)
		if mv, ok := v.(map[string]interface{}); ok {
			for k, v := range mv {
				newTupleRow.Set(k, v)
			}
		} else {
			newTupleRow.Set(srfName, v)
		}
		newData = append(newData, newTupleRow)
	}
	return newData, nil
}
