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

package function

import (
	"fmt"
	"reflect"

	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
)

type ResultCols map[string]interface{}

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
			return nil
		},
	}
}

func changedFunc(ctx api.FunctionContext, args []interface{}, keys []string) (ResultCols, error) {
	// validation
	if len(args) <= 2 {
		return nil, fmt.Errorf("expect more than two args but got %d", len(args))
	}
	prefix, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("first arg is not a string but got %v", args[0])
	}
	ignoreNull, ok := args[1].(bool)
	if !ok {
		return nil, fmt.Errorf("second arg is not a bool but got %v", args[1])
	}
	if len(args) != len(keys) {
		return nil, fmt.Errorf("the length of keys %d does not match the args %d", len(keys), len(args)-2)
	}

	var r ResultCols
	for i := 2; i < len(args); i++ {
		k := keys[i]
		v := args[i]
		if ignoreNull && v == nil {
			continue
		}
		lv, err := ctx.GetState(k)
		if err != nil {
			return nil, err
		}
		if !reflect.DeepEqual(v, lv) {
			if r == nil {
				r = make(ResultCols)
			}
			r[prefix+k] = v
			err := ctx.PutState(k, v)
			if err != nil {
				return nil, err
			}
		}
	}
	return r, nil
}
