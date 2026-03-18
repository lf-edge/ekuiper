// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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

package function

import (
	"fmt"
	"reflect"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)
type ResultCols struct {
	IndexValues []interface{}
	Keys        []string
}

// ColFunc Functions which will return columns directly instead of a map
type ColFunc func(ctx api.FunctionContext, args []interface{}, keys []string) (ResultCols, error)

func wrapColFunc(colFunc ColFunc) funcExe {
	return func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
		keys, ok := args[len(args)-1].([]string)
		if !ok {
			return fmt.Errorf("the last arg is not the key list but got %v", args[len(args)-1]), false
		}
		r, err := colFunc(ctx, args[:len(args)-1], keys)
		if err != nil {
			return err, false
		}
		return r, true
	}
}

func registerColsFunc() {
	builtins["changed_cols"] = builtinFunc{
		fType: ast.FuncTypeCols,
		exec:  wrapColFunc(changedFunc),
		val: func(_ api.FunctionContext, args []ast.Expr) error {
			if len(args) <= 2 {
				return fmt.Errorf("expect more than two args but got %d", len(args))
			}
			if ast.IsNumericArg(args[0]) || ast.IsTimeArg(args[0]) || ast.IsBooleanArg(args[0]) {
				return ProduceErrInfo(0, "string")
			}
			if ast.IsNumericArg(args[1]) || ast.IsTimeArg(args[1]) || ast.IsStringArg(args[1]) {
				return ProduceErrInfo(1, "bool")
			}
			for i := 2; i < len(args); i++ {
				if _, ok := args[i].(*ast.Wildcard); ok {
					return fmt.Errorf("wildcard * is not supported in changed_cols")
				}
			}
			return nil
		},
	}
}

func changedFunc(ctx api.FunctionContext, args []interface{}, keys []string) (ResultCols, error) {
	// validation
	if len(args) <= 2 {
		return ResultCols{}, fmt.Errorf("expect more than two args but got %d", len(args))
	}

	key := "all"
	v, err := ctx.GetState(key)
	if err != nil {
		return ResultCols{}, err
	}
	var states []interface{}
	changed := false
	if v != nil {
		states = v.([]interface{})
	} else {
		states = make([]interface{}, len(args))
		changed = true
	}
	if len(args) > len(states) {
		newStates := make([]interface{}, len(args))
		copy(newStates, states)
		states = newStates
		changed = true
	}

	var r ResultCols
	r.Keys = keys
	for i := 2; i < len(args); i++ {
		v := args[i]
		if v == nil {
			continue
		}
		if !isEqual(v, states[i]) {
			if r.IndexValues == nil {
				r.IndexValues = make([]interface{}, len(args))
			}
			r.IndexValues[i] = v
			states[i] = v
			changed = true
		}
	}
	if changed {
		err = ctx.PutState(key, states)
		if err != nil {
			return ResultCols{}, err
		}
	}
	return r, nil
}

func isEqual(v1, v2 interface{}) bool {
	if v1 == nil || v2 == nil {
		return v1 == v2
	}
	switch t1 := v1.(type) {
	case string:
		if t2, ok := v2.(string); ok {
			return t1 == t2
		}
	case int64:
		if t2, ok := v2.(int64); ok {
			return t1 == t2
		}
	case float64:
		if t2, ok := v2.(float64); ok {
			return t1 == t2
		}
	case bool:
		if t2, ok := v2.(bool); ok {
			return t1 == t2
		}
	case int:
		if t2, ok := v2.(int); ok {
			return t1 == t2
		}
	}
	return reflect.DeepEqual(v1, v2)
}
