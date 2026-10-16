// Copyright 2026 EMQ Technologies Co., Ltd.
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
	"math"
	"reflect"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

type SetOperator struct {
	Streams           []string
	OpType            ast.SetOpType
	LeftSelectFields  ast.Fields
	RightSelectFields ast.Fields
}

func (sp *SetOperator) Apply(ctx api.StreamContext, data interface{}, _ *xsql.FunctionValuer, _ *xsql.AggregateFunctionValuer) interface{} {
	var (
		input  xsql.Collection
		output xsql.Collection
	)

	switch v := data.(type) {
	case error:
		return v
	case xsql.Collection:
		input = v
	default:
		return fmt.Errorf("run set operator error: set operators must receive a collection")

	}

	if len(sp.Streams) < 2 {
		return fmt.Errorf("run set operator error: set operators must receive data from at least two streams")
	}

	lefts := input.GetBySrc(sp.Streams[0])
	rights := input.GetBySrc(sp.Streams[1])
	switch sp.OpType {
	case ast.UNION_OP:
		output = sp.union(ctx, false, lefts, rights)
	case ast.UNION_ALL:
		output = sp.union(ctx, true, lefts, rights)
	case ast.INTERSECT_OP:
		output = sp.intersect(ctx, false, lefts, rights)
	case ast.INTERSECT_ALL:
		output = sp.intersect(ctx, true, lefts, rights)
	case ast.EXCEPT_OP:
		output = sp.except(ctx, false, lefts, rights)
	case ast.EXCEPT_ALL:
		output = sp.except(ctx, true, lefts, rights)
	default:
		return fmt.Errorf("run set operator error: unknown set operator type")
	}

	// the result covers the same window as the input
	if wt, ok := output.(*xsql.WindowTuples); ok {
		wt.WindowRange = input.GetWindowRange()
	}

	return output
}

func (sp *SetOperator) union(ctx api.StreamContext, allowDuplicates bool, leftInputs, rightInputs []xsql.Row) xsql.Collection {
	if !allowDuplicates {
		leftInputs, rightInputs = sp.trimSelfDuplicates(ctx, leftInputs, rightInputs)
	}

	outputSet := &xsql.WindowTuples{}
	tuplesToSkip := make(map[int]struct{})
	for _, leftTuple := range leftInputs {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		if !allowDuplicates {
			for pos, rightTuple := range rightInputs {
				result := sp.equals(leftTuple, rightTuple)
				if result {
					tuplesToSkip[pos] = struct{}{}
					break
				}
			}
		}

		outputSet.AddTuple(leftTuple)
	}

	// attach the right stream values to the
	// output set
	for pos, rightTuple := range rightInputs {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		_, ok := tuplesToSkip[pos]
		if !ok {
			outputSet.AddTuple(rightTuple)
		}
	}

	return outputSet
}

func (sp *SetOperator) intersect(ctx api.StreamContext, allowDuplicates bool, leftInputs, rightInputs []xsql.Row) xsql.Collection {
	if !allowDuplicates {
		leftInputs, rightInputs = sp.trimSelfDuplicates(ctx, leftInputs, rightInputs)
	}

	outputSet := &xsql.WindowTuples{}
	used := make(map[int]struct{})
	for _, leftTuple := range leftInputs {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		for pos, rightTuple := range rightInputs {
			if _, ok := used[pos]; ok {
				continue
			}

			result := sp.equals(leftTuple, rightTuple)
			if result {
				used[pos] = struct{}{}
				outputSet.AddTuple(leftTuple)
				break
			}
		}
	}

	return outputSet
}

func (sp *SetOperator) except(ctx api.StreamContext, allowDuplicates bool, leftInputs, rightInputs []xsql.Row) xsql.Collection {
	if !allowDuplicates {
		leftInputs, rightInputs = sp.trimSelfDuplicates(ctx, leftInputs, rightInputs)
	}

	outputSet := &xsql.WindowTuples{}
	used := make(map[int]struct{})
	for _, leftTuple := range leftInputs {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		tupleFound := false
		for pos, rightTuple := range rightInputs {
			if _, ok := used[pos]; ok {
				continue
			}

			result := sp.equals(leftTuple, rightTuple)
			if result {
				used[pos] = struct{}{}
				tupleFound = true
				break
			}
		}

		if !tupleFound {
			outputSet.AddTuple(leftTuple)
		}
	}

	return outputSet
}

func (sp *SetOperator) trimSelfDuplicates(ctx api.StreamContext, left, right []xsql.Row) ([]xsql.Row, []xsql.Row) {
	trimmedLeft := make([]xsql.Row, 0)
	for _, tuple := range left {
		select {
		case <-ctx.Done():
			return nil, nil
		default:
		}

		isDuplicated := false
		for pos := range trimmedLeft {
			result := sp.isSelfEqual(tuple, trimmedLeft[pos], sp.LeftSelectFields)
			if result {
				isDuplicated = true
				break
			}
		}

		if !isDuplicated {
			trimmedLeft = append(trimmedLeft, tuple)
		}
	}

	trimmedRight := make([]xsql.Row, 0)
	for _, tuple := range right {
		select {
		case <-ctx.Done():
			return nil, nil
		default:
		}

		isDuplicated := false
		for pos := range trimmedRight {
			result := sp.isSelfEqual(tuple, trimmedRight[pos], sp.RightSelectFields)
			if result {
				isDuplicated = true
				break
			}
		}

		if !isDuplicated {
			trimmedRight = append(trimmedRight, tuple)
		}
	}

	return trimmedLeft, trimmedRight
}

func (sp *SetOperator) isSelfEqual(firstTuple, secondTuple xsql.Row, projection ast.Fields) bool {
	for _, field := range projection {
		if field.Invisible {
			continue
		}

		fvalue, _ := firstTuple.Value(field.GetName(), "")
		svalue, _ := secondTuple.Value(field.GetName(), "")

		if !reflect.DeepEqual(
			sp.normalizeData(fvalue),
			sp.normalizeData(svalue),
		) {
			return false
		}
	}
	return true
}

func (sp *SetOperator) equals(leftTuple, rightTuple xsql.Row) bool {
	// Invisible fields are not part of the output, so columns are matched by
	// their position among the visible fields of each side.
	leftFields, rightFields := sp.LeftSelectFields, sp.RightSelectFields
	l, r := 0, 0
	for {
		for l < len(leftFields) && leftFields[l].Invisible {
			l++
		}
		for r < len(rightFields) && rightFields[r].Invisible {
			r++
		}
		if l == len(leftFields) || r == len(rightFields) {
			// equal only if both sides ran out of visible columns together
			return l == len(leftFields) && r == len(rightFields)
		}

		lval, _ := leftTuple.Value(leftFields[l].GetName(), "")
		rval, _ := rightTuple.Value(rightFields[r].GetName(), "")
		if !reflect.DeepEqual(
			sp.normalizeData(lval),
			sp.normalizeData(rval),
		) {
			return false
		}
		l++
		r++
	}
}

func (sp *SetOperator) normalizeData(v any) any {
	switch t := v.(type) {
	case int:
		return int64(t)
	case int8:
		return int64(t)
	case int16:
		return int64(t)
	case int32:
		return int64(t)
	case uint:
		return normalizeUint(uint64(t))
	case uint8:
		return int64(t)
	case uint16:
		return int64(t)
	case uint32:
		return int64(t)
	case uint64:
		return normalizeUint(t)
	case float32:
		return normalizeFloat(float64(t))
	case float64:
		return normalizeFloat(t)
	case time.Time:
		return t.UnixNano()
	case map[string]any:
		m := make(map[string]any, len(t))
		for k, e := range t {
			m[k] = sp.normalizeData(e)
		}
		return m
	case []any:
		s := make([]any, len(t))
		for i, e := range t {
			s[i] = sp.normalizeData(e)
		}
		return s
	default:
		return v
	}
}

func normalizeFloat(f float64) any {
	if f == math.Trunc(f) && f >= math.MinInt64 && f < math.MaxInt64 {
		return int64(f)
	}
	return f
}

func normalizeUint(u uint64) any {
	if u <= math.MaxInt64 {
		return int64(u)
	}
	return u
}
