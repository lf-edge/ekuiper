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

func (ps *ProjectSetOperator) Apply(ctx api.StreamContext, data interface{}, fv *xsql.FunctionValuer, afv *xsql.AggregateFunctionValuer) interface{} {
	// for now we only support 1 srf function in the field
	srfName := ""
	for k := range ps.SrfMapping {
		srfName = k
		break
	}
	switch input := data.(type) {
	case error:
		return []interface{}{input}
	case xsql.TupleRow:
		aValue, ok := input.Value(srfName, "")
		if !ok {
			return fmt.Errorf("can't find the result from the %v function", srfName)
		}
		aValues, ok := aValue.([]interface{})
		if !ok {
			return fmt.Errorf("the result from the %v function should be array", srfName)
		}
		newData := make([]interface{}, 0)
		for _, v := range aValues {
			newTupleRow := input.Clone()
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
		return newData
	default:
		return []interface{}{fmt.Errorf("run Select error: invalid input %[1]T(%[1]v)", input)}
	}
}
