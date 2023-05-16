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
	"github.com/lf-edge/ekuiper/pkg/cast"
)

var (
	errorArrayArgumentError = fmt.Errorf("first argument should be array of interface{}")
	errorArrayIndex         = fmt.Errorf("index out of range")
)

func registerArrayFunc() {
	builtins["array_create"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			return args, true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			return nil
		},
	}
	builtins["array_position"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			array, ok := args[0].([]interface{})
			if !ok {
				return errorArrayArgumentError, false
			}
			for i, item := range array {
				if item == args[1] {
					return i, true
				}
			}
			return -1, true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			return ValidateLen(2, len(args))
		},
	}
	builtins["element_at"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			switch args[0].(type) {
			case []interface{}:
				array := args[0].([]interface{})
				index, err := cast.ToInt(args[1], cast.STRICT)
				if err != nil {
					return err, false
				}
				if index >= len(array) || -index > len(array) {
					return errorArrayIndex, false
				}
				if index >= 0 {
					return array[index], true
				}
				return array[len(array)+index], true
			case map[string]interface{}:
				m := args[0].(map[string]interface{})
				key, ok := args[1].(string)
				if !ok {
					return fmt.Errorf("second argument should be string"), false
				}
				return m[key], true
			default:
				return fmt.Errorf("first argument should be []interface{} or map[string]interface{}"), false
			}
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			return ValidateLen(2, len(args))
		},
	}
	builtins["array_contains"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			array, ok := args[0].([]interface{})
			if !ok {
				return errorArrayArgumentError, false
			}
			for _, item := range array {
				if item == args[1] {
					return true, true
				}
			}
			return false, true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			return ValidateLen(2, len(args))
		},
	}
}
