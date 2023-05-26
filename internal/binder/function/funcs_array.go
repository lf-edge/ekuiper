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
	"math"
	"math/rand"
	"strings"

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
	errorArraySecondArgumentNotStringError = fmt.Errorf("second argument should be string")
	errorArrayThirdArgumentNotIntError     = fmt.Errorf("third argument should be int")
	errorArrayThirdArgumentNotStringError  = fmt.Errorf("third argument should be string")
	errorArrayContainsNonNumOrBoolValError = fmt.Errorf("array contain elements that are not numeric or Boolean")
	errorArrayNotArrayElementError         = fmt.Errorf("array elements should be array")
	errorArrayNotStringElementError        = fmt.Errorf("array elements should be string")
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
			if !ok {
				return errorArrayFirstArgumentNotArrayError, false
			}

			index := 0

			for _, item := range array {
				if item != args[1] {
					array[index] = item
					index++
				}
			}
			return array[:index], true
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

			lastPos := -1
			for i := len(array) - 1; i >= 0; i-- {
				if array[i] == args[1] {
					lastPos = i
					break
				}
			}

			return lastPos, true
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

			capacity := len(array1)
			if len(array2) > capacity {
				capacity = len(array2)
			}

			intersection := make([]interface{}, 0, capacity)
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
			union := make([]interface{}, 0, len(array1)+len(array2))
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
			var res interface{}
			var maxVal float64 = math.Inf(-1)

			for _, val := range array {
				if val == nil {
					return nil, true
				}
				f, err := cast.ToFloat64(val, cast.CONVERT_ALL)
				if err != nil {
					return errorArrayContainsNonNumOrBoolValError, false
				}

				if f > maxVal {
					maxVal = f
					res = val
				}
			}
			return res, true
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
			var res interface{}
			var min float64 = math.Inf(1)

			for _, val := range array {
				if val == nil {
					return nil, true
				}

				f, err := cast.ToFloat64(val, cast.CONVERT_ALL)
				if err != nil {
					return errorArrayContainsNonNumOrBoolValError, false
				}

				if f < min {
					min = f
					res = val
				}
			}
			return res, true
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
			except := make([]interface{}, 0, len(array1))
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

				if step == 0 {
					return fmt.Errorf("invalid step: should not be zero"), false
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
	builtins["array_cardinality"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			array, ok := args[0].([]interface{})
			if !ok {
				return errorArrayFirstArgumentNotArrayError, false
			}
			return len(array), true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			return ValidateLen(1, len(args))
		},
	}
	builtins["array_flatten"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			array, ok := args[0].([]interface{})
			if !ok {
				return errorArrayFirstArgumentNotArrayError, false
			}

			var output []interface{}

			for _, val := range array {
				innerArr, ok := val.([]interface{})
				if !ok {
					return errorArrayNotArrayElementError, false
				}
				output = append(output, innerArr...)
			}

			return output, true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			return ValidateLen(1, len(args))
		},
	}
	builtins["array_distinct"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			array, ok := args[0].([]interface{})
			if !ok {
				return errorArrayFirstArgumentNotArrayError, false
			}

			output := make([]interface{}, 0, len(array))
			set := make(map[interface{}]bool)

			for _, val := range array {
				if !set[val] {
					output = append(output, val)
					set[val] = true
				}
			}

			return output, true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			return ValidateLen(1, len(args))
		},
	}
	builtins["array_map"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			funcName, ok := args[0].(string)
			if !ok {
				return fmt.Errorf("first argument should be string"), false
			}

			array, ok := args[1].([]interface{})
			if !ok {
				return errorArraySecondArgumentNotArrayError, false
			}

			mapped := make([]interface{}, 0, len(array))
			var result interface{}
			for _, v := range array {
				params := []interface{}{v}
				fs, ok := builtins[funcName]
				if !ok {
					return fmt.Errorf("unknown built-in function: %s.", funcName), false
				}

				if fs.fType != ast.FuncTypeScalar {
					return fmt.Errorf("first argument should be a scalar function."), false
				}
				eargs := make([]ast.Expr, len(params))
				if err := fs.val(nil, eargs); err != nil {
					return fmt.Errorf("function should accept exactly one argument."), false
				}

				result, ok = fs.exec(ctx, params)
				if !ok {
					return result, false
				}
				mapped = append(mapped, result)
			}

			return mapped, true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			return ValidateLen(2, len(args))
		},
	}
	builtins["array_join"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			array, ok := args[0].([]interface{})
			if !ok {
				return errorArrayFirstArgumentNotArrayError, false
			}

			delimiter, ok := args[1].(string)
			if !ok {
				return errorArraySecondArgumentNotStringError, false
			}

			var nullReplacement string
			if len(args) == 3 {
				nullReplacement, ok = args[2].(string)
				if !ok {
					return errorArrayThirdArgumentNotStringError, false
				}
			}

			for i, v := range array {
				if v == nil {
					array[i] = nullReplacement
				} else {
					array[i], ok = v.(string)
					if !ok {
						return errorArrayNotStringElementError, false
					}
				}
			}

			strs, err := cast.ToStringSlice(array, cast.CONVERT_ALL)
			if err != nil {
				return err, false
			}
			return strings.Join(strs, delimiter), true
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
	builtins["array_shuffle"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			array, ok := args[0].([]interface{})
			if !ok {
				return errorArrayFirstArgumentNotArrayError, false
			}

			for i := range array {
				j := rand.Intn(i + 1)
				array[i], array[j] = array[j], array[i]
			}

			return array, true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			return ValidateLen(1, len(args))
		},
	}
}
