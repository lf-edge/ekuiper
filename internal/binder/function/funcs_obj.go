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

package function

import (
	"fmt"

	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
)

func registerObjectFunc() {
	builtins["keys"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg := args[0]
			if arg, ok := arg.(map[string]interface{}); ok {
				list := make([]string, 0, len(arg))
				for key := range arg {
					list = append(list, key)
				}
				return list, true
			}
			return fmt.Errorf("the argument should be map[string]interface{}"), false
		},
		val:   ValidateOneArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["values"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg := args[0]
			if arg, ok := arg.(map[string]interface{}); ok {
				list := make([]interface{}, 0, len(arg))
				for _, value := range arg {
					list = append(list, value)
				}
				return list, true
			}
			return fmt.Errorf("the argument should be map[string]interface{}"), false
		},
		val:   ValidateOneArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["object"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			keys, ok := args[0].([]interface{})
			if !ok {
				return fmt.Errorf("first argument should be []string"), false
			}
			values, ok := args[1].([]interface{})
			if !ok {
				return fmt.Errorf("second argument should be []interface{}"), false
			}
			if len(keys) != len(values) {
				return fmt.Errorf("the length of the arguments should be same"), false
			}
			if len(keys) == 0 {
				return nil, true
			}
			m := make(map[string]interface{}, len(keys))
			for i, k := range keys {
				key, ok := k.(string)
				if !ok {
					return fmt.Errorf("first argument should be []string"), false
				}
				m[key] = values[i]
			}
			return m, true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			return ValidateLen(2, len(args))
		},
		check: returnNilIfHasAnyNil,
	}
	builtins["zip"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			lists, ok := args[0].([]interface{})
			if !ok {
				return fmt.Errorf("each argument should be [][2]interface{}"), false
			}
			if len(lists) == 0 {
				return nil, true
			}
			m := make(map[string]interface{}, len(lists))
			for _, item := range lists {
				a, ok := item.([]interface{})
				if !ok {
					return fmt.Errorf("each argument should be [][2]interface{}"), false
				}
				if len(a) != 2 {
					return fmt.Errorf("each argument should be [][2]interface{}"), false
				}
				key, ok := a[0].(string)
				if !ok {
					return fmt.Errorf("the first element in the list item should be string"), false
				}
				m[key] = a[1]
			}
			return m, true
		},
		val:   ValidateOneArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["items"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			m, ok := args[0].(map[string]interface{})
			if !ok {
				return fmt.Errorf("first argument should be map[string]interface{}"), false
			}
			if len(m) < 1 {
				return nil, true
			}
			list := make([]interface{}, 0, len(m))
			for k, v := range m {
				list = append(list, []interface{}{k, v})
			}
			return list, true
		},
		val:   ValidateOneArg,
		check: returnNilIfHasAnyNil,
	}
}
