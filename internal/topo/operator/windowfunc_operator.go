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
	"sort"

	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
)

type WindowFuncOperator struct {
	WindowFuncField ast.Field
}

type windowFuncHandle interface {
	handleTuple(input xsql.Row) xsql.Row
	handleCollection(input xsql.Collection) xsql.Collection
}

type rowNumberFuncHandle struct {
	name string
}

func (rh *rowNumberFuncHandle) handleTuple(input xsql.Row) xsql.Row {
	input.Set(rh.name, 1)
	return input
}

func (rh *rowNumberFuncHandle) handleCollection(input xsql.Collection) xsql.Collection {
	index := 1
	input.RangeSet(func(i int, r xsql.Row) (bool, error) {
		r.Set(rh.name, index)
		index++
		return true, nil
	})
	return input
}

func (wf *WindowFuncOperator) Apply(ctx api.StreamContext, data interface{}, fv *xsql.FunctionValuer, afv *xsql.AggregateFunctionValuer) interface{} {
	windowFuncField := wf.WindowFuncField
	name := windowFuncField.Name
	if windowFuncField.AName != "" {
		name = windowFuncField.AName
	}
	var funcName string
	var pr *ast.PartitionExpr
	var sortFields ast.SortFields
	switch c := windowFuncField.Expr.(type) {
	case *ast.Call:
		funcName = c.Name
		pr = c.Partition
		sortFields = c.SortFields
	case *ast.FieldRef:
		call := c.AliasRef.Expression.(*ast.Call)
		funcName = call.Name
		pr = call.Partition
		sortFields = call.SortFields
	}
	wh, err := getWindowFuncHandle(funcName, name)
	if err != nil {
		return err
	}
	switch input := data.(type) {
	case xsql.Row:
		wh.handleTuple(input)
	case xsql.Collection:
		if pr != nil {
			// handle the following case:
			// 1: row_number() over (partition by a)
			// 2: row_number() over (partition by a order by b)
			input, err = partitionCollection(ctx, input, fv, afv, pr, sortFields, wh)
			if err != nil {
				return err
			}
			return input
		} else if len(sortFields) > 0 {
			// handle the following case:
			// 1: row_number() over (order by a)
			input = sortCollection(ctx, input, fv, afv, sortFields)
			input = wh.handleCollection(input)
			return input
		}
		// handle the following case:
		// 1: row_number() without over clause
		input = wh.handleCollection(input)
		return input
	}
	return data
}

func getWindowFuncHandle(funcName, colName string) (windowFuncHandle, error) {
	switch funcName {
	case "row_number":
		return &rowNumberFuncHandle{name: colName}, nil
	}
	return nil, fmt.Errorf("")
}

func sortCollection(ctx api.StreamContext, data xsql.Collection, fv *xsql.FunctionValuer, afv *xsql.AggregateFunctionValuer, sortFields ast.SortFields) xsql.Collection {
	op := &OrderOp{SortFields: sortFields}
	output := op.Apply(ctx, data, fv, afv)
	return output.(xsql.Collection)
}

func partitionCollection(ctx api.StreamContext, input xsql.Collection, fv *xsql.FunctionValuer, afv *xsql.AggregateFunctionValuer, prs *ast.PartitionExpr, sortFields ast.SortFields, wh windowFuncHandle) (xsql.Collection, error) {
	result := make(map[string]*xsql.WindowTuples)
	keys := make([]string, 0)
	err := input.Range(func(i int, ir xsql.ReadonlyRow) (bool, error) {
		var name string
		tr := ir.(xsql.Row)
		ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(tr, &xsql.WindowRangeValuer{WindowRange: input.GetWindowRange()}, fv)}
		for _, pr := range prs.Exprs {
			r := ve.Eval(pr)
			if _, ok := r.(error); ok {
				return false, fmt.Errorf("run partition By error: %v", r)
			} else {
				name += fmt.Sprintf("%v,", r)
			}
		}
		if ts, ok := result[name]; !ok {
			keys = append(keys, name)
			result[name] = &xsql.WindowTuples{Content: []xsql.Row{tr}, WindowRange: input.GetWindowRange()}
		} else {
			ts.Content = append(ts.Content, tr)
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	output := &xsql.WindowTuples{Content: []xsql.Row{}, WindowRange: input.GetWindowRange()}
	// visit result by order
	sort.Strings(keys)
	for _, key := range keys {
		subOutput := sortCollection(ctx, result[key], fv, afv, sortFields)
		subOutput = wh.handleCollection(subOutput)
		subOutput.Range(func(i int, r xsql.ReadonlyRow) (bool, error) {
			t := r.(xsql.Row)
			output.AddTuple(t)
			return true, nil
		})
	}
	return output, nil
}
