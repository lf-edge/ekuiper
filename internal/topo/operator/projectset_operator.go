// Copyright 2023-2024 EMQ Technologies Co., Ltd.
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

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/xsql"
)

type ProjectSetOperator struct {
	SrfMapping  map[string]struct{}
	EnableLimit bool
	LimitCount  int
}

// Apply implement UnOperation
// ProjectSetOperator will extract the results from the set-returning-function into multi rows by aligning other columns
// For tuple, ProjectSetOperator will do the following transform:
// {"a":[1,2],"b":3} => {"a":1,"b":3},{"a":2,"b":3}
// For Collection, ProjectSetOperator will do the following transform:
// [{"a":[1,2],"b":3},{"a":[1,2],"b":4}] = > [{"a":"1","b":3},{"a":"2","b":3},{"a":"1","b":4},{"a":"2","b":4}]
func (ps *ProjectSetOperator) Apply(ctx api.StreamContext, data interface{}, _ *xsql.FunctionValuer, _ *xsql.AggregateFunctionValuer) interface{} {
	if ps.LimitCount == 0 && ps.EnableLimit {
		return []xsql.Row{}
	}
	switch input := data.(type) {
	case error:
		return input
	case xsql.Row:
		results, err := ps.handleSRFRow(ctx, input)
		if err != nil {
			return err
		}
		if ps.EnableLimit && ps.LimitCount > 0 && len(results.rows) > ps.LimitCount {
			return results.rows[:ps.LimitCount]
		}
		return results.rows
	case xsql.Collection:
		if ps.EnableLimit && ps.LimitCount > 0 && input.Len() > ps.LimitCount {
			sel := make([]int, 0, ps.LimitCount)
			for i := 0; i < ps.LimitCount; i++ {
				sel = append(sel, i)
			}
			input = input.Filter(sel)
		}
		if err := ps.handleSRFRowForCollection(ctx, input); err != nil {
			return err
		}
		if ps.EnableLimit && ps.LimitCount > 0 && input.Len() > ps.LimitCount {
			sel := make([]int, ps.LimitCount)
			for i := 0; i < ps.LimitCount; i++ {
				sel[i] = i
			}
			return input.Filter(sel)
		}
		return input
	default:
		return fmt.Errorf("run Select error: invalid input %[1]T(%[1]v)", input)
	}
}

func (ps *ProjectSetOperator) handleSRFRowForCollection(ctx api.StreamContext, data xsql.Collection) error {
	switch collection := data.(type) {
	case *xsql.JoinTuples:
		newContent := make([]*xsql.JoinTuple, 0)
		for _, c := range collection.Content {
			rs, err := ps.handleSRFRow(ctx, c)
			if err != nil {
				return err
			}
			newContent = append(newContent, rs.joinTuples...)
		}
		collection.Content = newContent
	case *xsql.GroupedTuplesSet:
		newGroups := make([]*xsql.GroupedTuples, 0)
		for _, c := range collection.Groups {
			rs, err := ps.handleSRFRow(ctx, c)
			if err != nil {
				return err
			}
			newGroups = append(newGroups, rs.groupTuples...)
		}
		collection.Groups = newGroups
	case *xsql.WindowTuples:
		newContent := make([]xsql.Row, 0)
		for _, c := range collection.Content {
			rs, err := ps.handleSRFRow(ctx, c)
			if err != nil {
				return err
			}
			newContent = append(newContent, rs.rows...)
		}
		collection.Content = newContent
	default:
		return fmt.Errorf("run Select error: invalid input %[1]T(%[1]v)", data)
	}
	return nil
}

func (ps *ProjectSetOperator) handleSRFRow(ctx api.StreamContext, row xsql.Row) (*resultWrapper, error) {
	// for now we only support 1 srf function in the field
	srfName := ""
	for k := range ps.SrfMapping {
		srfName = k
		break
	}
	aValue, ok := row.Value(srfName, "")
	if !ok {
		return nil, fmt.Errorf("can't find the result from the %v function", srfName)
	}
	aValues, ok := aValue.([]interface{})
	if !ok {
		return nil, fmt.Errorf("the argument for the %v function should be array", srfName)
	}
	res := newResultWrapper(len(aValues), row)
	for i, v := range aValues {
		newRow := row.Clone()
		newRow.SetTracerCtx(row.GetTracerCtx())
		// clear original column value
		newRow.Del(srfName)
		if mv, ok := v.(map[string]interface{}); ok {
			for k, v := range mv {
				newRow.Set(k, v)
			}
		} else {
			newRow.Set(srfName, v)
		}
		res.appendTuple(i, newRow)
	}
	return res, nil
}

type resultWrapper struct {
	joinTuples  []*xsql.JoinTuple
	groupTuples []*xsql.GroupedTuples
	rows        []xsql.Row
}

func newResultWrapper(len int, row xsql.Row) *resultWrapper {
	r := &resultWrapper{}
	switch row.(type) {
	case *xsql.JoinTuple:
		r.joinTuples = make([]*xsql.JoinTuple, len)
	case *xsql.GroupedTuples:
		r.groupTuples = make([]*xsql.GroupedTuples, len)
	case xsql.Row:
		r.rows = make([]xsql.Row, len)
	}
	return r
}

func (r *resultWrapper) appendTuple(index int, newRow xsql.Row) {
	switch row := newRow.(type) {
	case *xsql.JoinTuple:
		r.joinTuples[index] = row
	case *xsql.GroupedTuples:
		r.groupTuples[index] = row
	case xsql.Row:
		r.rows[index] = row
	}
}
