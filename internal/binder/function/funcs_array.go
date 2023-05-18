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

var (
	errorArrayFirstArgumentNotArrayError   = fmt.Errorf("first argument should be array of interface{}")
	errorArrayIndex                        = fmt.Errorf("index out of range")
	errorArraySecondArgumentNotArrayError  = fmt.Errorf("second argument should be array of interface{}")
	errorArrayFirstArgumentNotIntError     = fmt.Errorf("first argument should be int")
	errorArraySecondArgumentNotIntError    = fmt.Errorf("second argument should be int")
	errorArrayThirdArgumentNotIntError     = fmt.Errorf("third argument should be int")
	errorArrayContainsNonNumOrBoolValError = fmt.Errorf("array contain elements that are not numeric or Boolean")
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
				return errorArrayFirstArgumentNotArrayError, false
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
				return errorArrayFirstArgumentNotArrayError, false
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

	builtins["array_remove"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			array, ok := args[0].([]interface{})
			res := make([]interface{}, 0, len(array))
			if !ok {
				return errorArrayFirstArgumentNotArrayError, false
			}
			for _, item := range array {
				if item != args[1] {
					res = append(res, item)
				}
			}
			return res, true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			return ValidateLen(2, len(args))
		},
	}

	builtins["array_last_position"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			array, ok := args[0].([]interface{})
			if !ok {
				return errorArrayFirstArgumentNotArrayError, false
			}

			for i := len(array) - 1; i >= 0; i-- {
				if array[i] == args[1] {
					return i + 1, true
				}
			}

			return 0, true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			return ValidateLen(2, len(args))
		},
	}

	builtins["array_contains_any"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			array1, ok1 := args[0].([]interface{})
			if !ok1 {
				return errorArrayFirstArgumentNotArrayError, false
			}
			array2, ok2 := args[1].([]interface{})
			if !ok2 {
				return errorArraySecondArgumentNotArrayError, false
			}

			for _, a := range array1 {
				for _, b := range array2 {
					if a == b {
						return true, true
					}
				}
			}

			return false, true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			return ValidateLen(2, len(args))
		},
	}

	builtins["array_intersect"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			array1, ok1 := args[0].([]interface{})
			if !ok1 {
				return errorArrayFirstArgumentNotArrayError, false
			}
			array2, ok2 := args[1].([]interface{})
			if !ok2 {
				return errorArraySecondArgumentNotArrayError, false
			}
			intersection := []interface{}{}
			set := make(map[interface{}]bool)

			for _, a := range array1 {
				set[a] = true
			}

			for _, b := range array2 {
				if set[b] {
					intersection = append(intersection, b)
					set[b] = false
				}
			}

			return intersection, true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			return ValidateLen(2, len(args))
		},
	}

	builtins["array_union"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			array1, ok1 := args[0].([]interface{})
			if !ok1 {
				return errorArrayFirstArgumentNotArrayError, false
			}
			array2, ok2 := args[1].([]interface{})
			if !ok2 {
				return errorArraySecondArgumentNotArrayError, false
			}
			union := []interface{}{}
			set := make(map[interface{}]bool)

			for _, a := range array1 {
				if !set[a] {
					union = append(union, a)
					set[a] = true
				}
			}
			for _, b := range array2 {
				if !set[b] {
					set[b] = true
					union = append(union, b)
				}
			}

			return union, true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			return ValidateLen(2, len(args))
		},
	}
	builtins["array_max"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			array, ok := args[0].([]interface{})
			if !ok {
				return errorArrayFirstArgumentNotArrayError, false
			}
			var max interface{}
			var maxType reflect.Type

			for _, val := range array {
				if val == nil {
					return nil, true
				}
				v := reflect.ValueOf(val)
				switch v.Kind() {
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
					reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					f, _ := cast.ToFloat64(val, cast.CONVERT_SAMEKIND)
					if max == nil || f > reflect.ValueOf(max).Convert(reflect.TypeOf(f)).Float() {
						max = f
						maxType = v.Type()
					}
				case reflect.Float32, reflect.Float64:
					if max == nil || v.Float() > reflect.ValueOf(max).Float() {
						max = val
						maxType = v.Type()
					}
				case reflect.Bool:
					b := v.Bool()
					if max == nil || (b && !reflect.ValueOf(max).Bool()) {
						max = val
						maxType = v.Type()
					}
				default:
					return errorArrayContainsNonNumOrBoolValError, false
				}

			}
			return reflect.ValueOf(max).Convert(maxType).Interface(), true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			return ValidateLen(1, len(args))
		},
	}
	builtins["array_min"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			array, ok := args[0].([]interface{})
			if !ok {
				return errorArrayFirstArgumentNotArrayError, false
			}
			var min interface{}
			var minType reflect.Type

			for _, val := range array {
				if val == nil {
					return nil, true
				}
				v := reflect.ValueOf(val)
				switch v.Kind() {
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
					reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					f, _ := cast.ToFloat64(val, cast.CONVERT_SAMEKIND)
					if min == nil || f < reflect.ValueOf(min).Convert(reflect.TypeOf(f)).Float() {
						min = f
						minType = v.Type()
					}
				case reflect.Float32, reflect.Float64:
					if min == nil || v.Float() < reflect.ValueOf(min).Float() {
						min = val
						minType = v.Type()
					}
				case reflect.Bool:
					b := v.Bool()
					if min == nil || (b && !reflect.ValueOf(min).Bool()) {
						min = val
						minType = v.Type()
					}
				default:
					return errorArrayContainsNonNumOrBoolValError, false
				}

			}
			return reflect.ValueOf(min).Convert(minType).Interface(), true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			return ValidateLen(1, len(args))
		},
	}
	builtins["array_except"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			array1, ok1 := args[0].([]interface{})
			if !ok1 {
				return errorArrayFirstArgumentNotArrayError, false
			}
			array2, ok2 := args[1].([]interface{})
			if !ok2 {
				return errorArraySecondArgumentNotArrayError, false
			}
			except := []interface{}{}
			set := make(map[interface{}]bool)

			for _, v := range array2 {
				set[v] = true
			}

			for _, v := range array1 {
				if !set[v] {
					except = append(except, v)
					set[v] = true
				}
			}

			return except, true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			return ValidateLen(2, len(args))
		},
	}
	builtins["repeat"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			elemt, ok := args[0].(interface{})
			if !ok {
				return errorArrayFirstArgumentNotArrayError, false
			}
			count, ok := args[1].(int)
			if !ok {
				return errorArraySecondArgumentNotIntError, false
			}

			arr := make([]interface{}, count)
			for i := range arr {
				arr[i] = elemt
			}

			return arr, true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			return ValidateLen(2, len(args))
		},
	}
	builtins["sequence"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			var step, start, stop int
			var ok bool
			start, ok = args[0].(int)
			if !ok {
				return errorArrayFirstArgumentNotIntError, false
			}
			stop, ok = args[1].(int)
			if !ok {
				return errorArraySecondArgumentNotIntError, false
			}
			if len(args) == 3 {
				step, ok = args[2].(int)
				if !ok {
					return errorArrayThirdArgumentNotIntError, false
				}

			} else {
				if start < stop {
					step = 1
				} else {
					step = -1
				}
			}

			n := (stop-start)/step + 1

			arr := make([]interface{}, n)
			for i := range arr {
				arr[i] = start + i*step
			}

			return arr, true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(2, len(args)); err != nil {
				if err := ValidateLen(3, len(args)); err != nil {
					return fmt.Errorf("Expect two or three arguments but found %d.", len(args))
				}
			}
			return nil
		},
	}
}
