// Copyright 2022-2023 EMQ Technologies Co., Ltd.
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
	"reflect"
	"strconv"
)

// registerAnalyticFunc registers the analytic functions
// The last parameter of the function is always the partition key
func registerAnalyticFunc() {
	builtins["changed_col"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			ignoreNull, ok := args[0].(bool)
			if !ok {
				return fmt.Errorf("first arg is not a bool but got %v", args[0]), false
			}
			if ignoreNull && args[1] == nil {
				return nil, true
			}
			key := args[len(args)-1].(string)
			lv, err := ctx.GetState(key)
			if err != nil {
				return err, false
			}
			if !reflect.DeepEqual(args[1], lv) {
				err := ctx.PutState(key, args[1])
				if err != nil {
					return err, false
				}
				return args[1], true
			}
			return nil, true
		},
		val: func(_ api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(2, len(args)); err != nil {
				return err
			}
			if ast.IsNumericArg(args[0]) || ast.IsTimeArg(args[0]) || ast.IsStringArg(args[0]) {
				return ProduceErrInfo(0, "boolean")
			}
			return nil
		},
	}
	builtins["had_changed"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			l := len(args) - 1
			if l <= 1 {
				return fmt.Errorf("expect more than one arg but got %d", len(args)), false
			}
			ignoreNull, ok := args[0].(bool)
			if !ok {
				return fmt.Errorf("first arg is not a bool but got %v", args[0]), false
			}
			key := args[l].(string)
			result := false
			for i := 1; i < l; i++ {
				v := args[i]
				k := key + strconv.Itoa(i)
				if ignoreNull && v == nil {
					continue
				}
				lv, err := ctx.GetState(k)
				if err != nil {
					return fmt.Errorf("error getting state for %s: %v", k, err), false
				}
				if !reflect.DeepEqual(v, lv) {
					result = true
					err := ctx.PutState(k, v)
					if err != nil {
						return fmt.Errorf("error setting state for %s: %v", k, err), false
					}
				}
			}
			return result, true
		},
		val: func(_ api.FunctionContext, args []ast.Expr) error {
			if len(args) <= 1 {
				return fmt.Errorf("expect more than one arg but got %d", len(args))
			}
			if ast.IsNumericArg(args[0]) || ast.IsTimeArg(args[0]) || ast.IsStringArg(args[0]) {
				return ProduceErrInfo(0, "bool")
			}
			return nil
		},
	}

	builtins["lag"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			l := len(args) - 1
			key := args[l].(string)
			if l != 1 && l != 2 && l != 3 {
				return fmt.Errorf("expect one two or three args but got %d", l), false
			}
			v, err := ctx.GetState(key)
			if err != nil {
				return fmt.Errorf("error getting state for %s: %v", key, err), false
			}
			if v == nil {
				size := 0
				var dftVal interface{} = nil
				if l == 3 {
					dftVal = args[2]
				}
				// first time call, need create state for lag
				if l == 1 {
					size = 1
				} else {
					size, err = cast.ToInt(args[1], cast.STRICT)
					if err != nil {
						return fmt.Errorf("error converting second arg %v to int: %v", args[1], err), false
					}
				}

				rq := newRingqueue(size)
				rq.fill(dftVal)

				rtnVal, _ := rq.fetch()
				rq.append(args[0])
				err := ctx.PutState(key, rq)
				if err != nil {
					return fmt.Errorf("error setting state for %s: %v", key, err), false
				}
				return rtnVal, true
			} else {
				rq, ok := v.(*ringqueue)
				if !ok {
					return fmt.Errorf("error getting state for %s: %v", key, err), false
				}
				rtnVal, _ := rq.fetch()
				rq.append(args[0])
				err := ctx.PutState(key, rq)
				if err != nil {
					return fmt.Errorf("error setting state for %s: %v", key, err), false
				}
				return rtnVal, true
			}
		},
		val: func(_ api.FunctionContext, args []ast.Expr) error {
			l := len(args)
			if l != 1 && l != 2 && l != 3 {
				return fmt.Errorf("expect one two or three args but got %d", l)
			}
			if l >= 2 {
				if ast.IsFloatArg(args[1]) || ast.IsTimeArg(args[1]) || ast.IsBooleanArg(args[1]) || ast.IsStringArg(args[1]) || ast.IsFieldRefArg(args[1]) {
					return ProduceErrInfo(1, "int")
				}
				if s, ok := args[1].(*ast.IntegerLiteral); ok {
					if s.Val < 0 {
						return fmt.Errorf("the index should not be a nagtive integer")
					}
				}
			}
			return nil
		},
	}

	builtins["latest"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			l := len(args) - 1
			key := args[l].(string)
			if l != 1 && l != 2 {
				return fmt.Errorf("expect one or two args but got %d", l), false
			}
			if args[0] == nil {
				v, err := ctx.GetState(key)
				if err != nil {
					return fmt.Errorf("error getting state for %s: %v", key, err), false
				}
				if v == nil {
					if l == 2 {
						return args[1], true
					} else {
						return nil, true
					}
				} else {
					return v, true
				}
			} else {
				ctx.PutState(key, args[0])
				return args[0], true
			}
		},
		val: func(_ api.FunctionContext, args []ast.Expr) error {
			l := len(args)
			if l != 1 && l != 2 {
				return fmt.Errorf("expect one or two args but got %d", l)
			}
			return nil
		},
	}
}
