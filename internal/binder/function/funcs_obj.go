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
	"reflect"

	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/cast"
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
	builtins["object_concat"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			res := make(map[string]interface{})
			for i, arg := range args {
				if arg == nil {
					continue
				}
				arg, ok := arg.(map[string]interface{})
				if !ok {
					return fmt.Errorf("the argument should be map[string]interface{}, got %v", args[i]), false
				}
				for k, v := range arg {
					res[k] = v
				}
			}
			return res, true
		},
		val: func(_ api.FunctionContext, args []ast.Expr) error {
			return ValidateAtLeast(2, len(args))
		},
	}
	builtins["object_construct"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			result := make(map[string]interface{})
			for i := 0; i < len(args); i += 2 {
				if args[i+1] != nil {
					s, err := cast.ToString(args[i], cast.CONVERT_SAMEKIND)
					if err != nil {
						return fmt.Errorf("key %v is not a string", args[i]), false
					}
					result[s] = args[i+1]
				}
			}
			return result, true
		},
		val: func(_ api.FunctionContext, args []ast.Expr) error {
			if len(args)%2 != 0 {
				return fmt.Errorf("the args must be key value pairs")
			}
			for i, arg := range args {
				if i%2 == 0 {
					if ast.IsNumericArg(arg) || ast.IsTimeArg(arg) || ast.IsBooleanArg(arg) {
						return ProduceErrInfo(i, "string")
					}
				}
			}
			return nil
		},
	}
	builtins["erase"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if len(args) != 2 {
				return fmt.Errorf("the argument number should be 2, got %v", len(args)), false
			}
			res := make(map[string]interface{})
			argMap, ok := args[0].(map[string]interface{})
			if !ok {
				return fmt.Errorf("the first argument should be map[string]interface{}, got %v", args[0]), false
			}
			eraseArray := make([]string, 0)
			v := reflect.ValueOf(args[1])
			switch v.Kind() {
			case reflect.Slice:
				array, err := cast.ToStringSlice(args[1], cast.CONVERT_ALL)
				if err != nil {
					return err, false
				}
				eraseArray = append(eraseArray, array...)
			case reflect.String:
				str := args[1].(string)
				for k, v := range argMap {
					if k != str {
						res[k] = v
					}
				}
				return res, true
			default:
				return fmt.Errorf("the augument should be slice or string"), false
			}
			for k, v := range argMap {
				if !sliceStringContains(eraseArray, k) {
					res[k] = v
				}
			}

			return res, true
		},
		val: func(_ api.FunctionContext, args []ast.Expr) error {
			return ValidateAtLeast(2, len(args))
		},
		check: returnNilIfHasAnyNil,
	}
	builtins["object_pick"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if len(args) != 2 {
				return fmt.Errorf("the argument number should be 2, got %v", len(args)), false
			}
			res := make(map[string]interface{})
			argMap, ok := args[0].(map[string]interface{})
			if !ok {
				return fmt.Errorf("the first argument should be map[string]interface{}, got %v", args[0]), false
			}
			pickArray := make([]string, 0)
			v := reflect.ValueOf(args[1])
			switch v.Kind() {
			case reflect.Slice:
				array, err := cast.ToStringSlice(args[1], cast.CONVERT_ALL)
				if err != nil {
					return err, false
				}
				pickArray = append(pickArray, array...)
			case reflect.String:
				str := args[1].(string)
				for k, v := range argMap {
					if k == str {
						res[k] = v
					}
				}
				return res, true
			default:
				return fmt.Errorf("the augument should be slice or string"), false
			}
			for k, v := range argMap {
				if sliceStringContains(pickArray, k) {
					res[k] = v
				}
			}

			return res, true
		},
		val: func(_ api.FunctionContext, args []ast.Expr) error {
			return ValidateLen(2, len(args))
		},
		check: returnNilIfHasAnyNil,
	}
	builtins["obj_to_kvpair_array"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			obj, ok := args[0].(map[string]interface{})
			if !ok {
				return fmt.Errorf("the first argument should be map[string]interface{}, got %v", args[0]), false
			}

			res := make([]interface{}, 0, len(obj))
			for k, v := range obj {
				pair := make(map[string]interface{}, 2)
				pair[kvPairKName] = k
				pair[kvPairVName] = v
				res = append(res, pair)
			}
			return res, true
		},
		val:   ValidateOneArg,
		check: returnNilIfHasAnyNil,
	}
}

func sliceStringContains(s []string, target string) bool {
	for _, v := range s {
		if target == v {
			return true
		}
	}
	return false
}
