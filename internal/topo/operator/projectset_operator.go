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
	srfName    string

	collectionRows []xsql.CloneAbleRow
}

// Apply implement UnOperation
// ProjectSetOperator will extract the results from the set-returning-function into multi rows by aligning other columns
// For tuple, ProjectSetOperator will do the following transform:
// {"a":[1,2],"b":3} => {"a":1,"b":3},{"a":2,"b":3}
// For Collection, ProjectSetOperator will do the following transform:
// [{"a":[1,2],"b":3},{"a":[1,2],"b":4}] = > [{"a":"1","b":3},{"a":"2","b":3},{"a":"1","b":4},{"a":"2","b":4}]
func (ps *ProjectSetOperator) Apply(_ api.StreamContext, data interface{}, _ *xsql.FunctionValuer, _ *xsql.AggregateFunctionValuer) interface{} {
	// for now we only support 1 srf function in the field
	srfName := ""
	for k := range ps.SrfMapping {
		srfName = k
		break
	}
	ps.srfName = srfName
	ps.collectionRows = make([]xsql.CloneAbleRow, 0)
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
		err := input.Range(ps.handleSRFRowForCollection)
		if err != nil {
			return err
		}
		switch ts := input.(type) {
		case *xsql.JoinTuples:
			newTuples := make([]*xsql.JoinTuple, 0)
			for _, tuple := range ps.collectionRows {
				newTuples = append(newTuples, tuple.(*xsql.JoinTuple))
			}
			ts.Content = newTuples
		case *xsql.GroupedTuplesSet:
			newTuples := make([]*xsql.GroupedTuples, 0)
			for _, tuple := range ps.collectionRows {
				newTuples = append(newTuples, tuple.(*xsql.GroupedTuples))
			}
			ts.Groups = newTuples
		case *xsql.WindowTuples:
			newTuples := make([]xsql.TupleRow, 0)
			for _, tuple := range ps.collectionRows {
				newTuples = append(newTuples, tuple.(xsql.TupleRow))
			}
			ts.Content = newTuples
		}
		return input
	default:
		return fmt.Errorf("run Select error: invalid input %[1]T(%[1]v)", input)
	}
}

func (ps *ProjectSetOperator) handleSRFRowForCollection(i int, r xsql.CloneAbleRow) (bool, error) {
	rows, err := ps.handleSRFRow(ps.srfName, r)
	if err != nil {
		return false, err
	}
	ps.collectionRows = append(ps.collectionRows, rows...)
	return true, nil
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
