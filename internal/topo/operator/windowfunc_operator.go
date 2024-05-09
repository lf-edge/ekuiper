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

	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
)

type WindowFuncOperator struct {
	WindowFuncField ast.Field
}

type windowFuncHandle interface {
	handleTuple(input xsql.Row)
	handleCollection(input xsql.Collection)
}

type rowNumberFuncHandle struct {
	name string
}

func (rh *rowNumberFuncHandle) handleTuple(input xsql.Row) {
	input.Set(rh.name, 1)
}

func (rh *rowNumberFuncHandle) handleCollection(input xsql.Collection) {
	index := 1
	input.RangeSet(func(i int, r xsql.Row) (bool, error) {
		r.Set(rh.name, index)
		index++
		return true, nil
	})
}

func (wf *WindowFuncOperator) Apply(_ api.StreamContext, data interface{}, _ *xsql.FunctionValuer, _ *xsql.AggregateFunctionValuer) interface{} {
	windowFuncField := wf.WindowFuncField
	name := windowFuncField.Name
	if windowFuncField.AName != "" {
		name = windowFuncField.AName
	}
	var funcName string
	switch c := windowFuncField.Expr.(type) {
	case *ast.Call:
		funcName = c.Name
	case *ast.FieldRef:
		funcName = c.AliasRef.Expression.(*ast.Call).Name
	}
	wh, err := getWindowFuncHandle(funcName, name)
	if err != nil {
		return err
	}
	switch input := data.(type) {
	case xsql.Row:
		wh.handleTuple(input)
	case xsql.Collection:
		wh.handleCollection(input)
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
