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
	"math"
	"reflect"
	"strconv"

	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/cast"
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
			validData, ok := args[len(args)-2].(bool)
			if !ok {
				return fmt.Errorf("when arg is not a bool but got %v", args[len(args)-2]), false
			}
			if !validData {
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
			l := len(args) - 2
			if l <= 1 {
				return fmt.Errorf("expect more than one arg but got %d", len(args)), false
			}
			validData, ok := args[len(args)-2].(bool)
			if !ok {
				return fmt.Errorf("when arg is not a bool but got %v", args[len(args)-2]), false
			}
			if !validData {
				return false, true
			}
			ignoreNull, ok := args[0].(bool)
			if !ok {
				return fmt.Errorf("first arg is not a bool but got %v", args[0]), false
			}
			key := args[len(args)-1].(string)
			paraLen := len(args) - 2
			result := false
			for i := 1; i < paraLen; i++ {
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
			l := len(args) - 2
			if l != 1 && l != 2 && l != 3 {
				return fmt.Errorf("expect one two or three args but got %d", l), false
			}
			key := args[len(args)-1].(string)
			v, err := ctx.GetState(key)
			if err != nil {
				return fmt.Errorf("error getting state for %s: %v", key, err), false
			}
			validData, ok := args[len(args)-2].(bool)
			if !ok {
				return fmt.Errorf("when arg is not a bool but got %v", args[len(args)-2]), false
			}
			paraLen := len(args) - 2
			var rq *ringqueue = nil
			var rtnVal interface{} = nil

			// first time call, need create state for lag
			if v == nil {
				size := 0
				var dftVal interface{} = nil
				if paraLen == 3 {
					dftVal = args[2]
				}
				if paraLen == 1 {
					size = 1
				} else {
					size, err = cast.ToInt(args[1], cast.STRICT)
					if err != nil {
						return fmt.Errorf("error converting second arg %v to int: %v", args[1], err), false
					}
				}
				rq = newRingqueue(size)
				rq.fill(dftVal)
				err := ctx.PutState(key, rq)
				if err != nil {
					return fmt.Errorf("error setting state for %s: %v", key, err), false
				}
			} else {
				rq, _ = v.(*ringqueue)
			}

			if validData {
				rtnVal, _ = rq.fetch()
				rq.append(args[0])
				err := ctx.PutState(key, rq)
				if err != nil {
					return fmt.Errorf("error setting state for %s: %v", key, err), false
				}
			} else {
				rtnVal, _ = rq.peek()
			}
			return rtnVal, true
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
			l := len(args) - 2
			if l != 1 && l != 2 {
				return fmt.Errorf("expect one or two args but got %d", l), false
			}
			paraLen := len(args) - 2
			key := args[len(args)-1].(string)
			validData, ok := args[len(args)-2].(bool)
			if !ok {
				return fmt.Errorf("when arg is not a bool but got %v", args[len(args)-2]), false
			}
			// notice nil is ignored in latest
			if validData && args[0] != nil {
				ctx.PutState(key, args[0])
				return args[0], true
			} else {
				v, err := ctx.GetState(key)
				if err != nil {
					return fmt.Errorf("error getting state for %s: %v", key, err), false
				}
				if v == nil {
					if paraLen == 2 {
						return args[1], true
					} else {
						return nil, true
					}
				} else {
					return v, true
				}
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

func registerGlobalAggFunc() {
	builtins["acc_avg"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			key := args[len(args)-1].(string)
			keyCount := fmt.Sprintf("%s_count", key)
			keySum := fmt.Sprintf("%s_sum", key)

			v1, err := ctx.GetState(keyCount)
			if err != nil {
				return err, false
			}
			v2, err := ctx.GetState(keySum)
			if err != nil {
				return err, false
			}
			validData, ok := args[len(args)-2].(bool)
			if !ok {
				return fmt.Errorf("when arg is not a bool but got %v", args[len(args)-2]), false
			}
			if v1 == nil && v2 == nil {
				if args[0] == nil || !validData {
					return 0, true
				}

				v1 = float64(0)
				v2 = float64(0)
			} else {
				if args[0] == nil || !validData {
					count := v1.(float64)
					sum := v2.(float64)
					return sum / count, true
				}
			}
			count := v1.(float64)
			sum := v2.(float64)
			count = count + 1
			switch v := args[0].(type) {
			case int:
				sum += float64(v)
			case int32:
				sum += float64(v)
			case int64:
				sum += float64(v)
			case float32:
				sum += float64(v)
			case float64:
				sum += v
			default:
				return fmt.Errorf("the value should be number"), false
			}
			if err := ctx.PutState(keyCount, count); err != nil {
				return err, false
			}
			if err := ctx.PutState(keySum, sum); err != nil {
				return err, false
			}
			return sum / count, true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			return nil
		},
	}
	builtins["acc_max"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			key := args[len(args)-1].(string)
			val, err := ctx.GetState(key)
			if err != nil {
				return err, false
			}
			validData, ok := args[len(args)-2].(bool)
			if !ok {
				return fmt.Errorf("when arg is not a bool but got %v", args[len(args)-2]), false
			}
			if val == nil {
				if !validData {
					return nil, false
				}
				val = float64(math.MinInt64)
			}
			m := val.(float64)
			if !validData {
				return m, true
			}
			switch v := args[0].(type) {
			case int:
				v1 := float64(v)
				m = getMax(m, v1)
			case int32:
				v1 := float64(v)
				m = getMax(m, v1)
			case int64:
				v1 := float64(v)
				m = getMax(m, v1)
			case float32:
				v1 := float64(v)
				m = getMax(m, v1)
			case float64:
				m = getMax(m, v)
			default:
				return fmt.Errorf("the value should be number"), false
			}
			if err := ctx.PutState(key, m); err != nil {
				return err, false
			}
			return m, true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			return ValidateLen(1, len(args))
		},
	}
	builtins["acc_min"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			key := args[len(args)-1].(string)
			val, err := ctx.GetState(key)
			if err != nil {
				return err, false
			}
			validData, ok := args[len(args)-2].(bool)
			if !ok {
				return fmt.Errorf("when arg is not a bool but got %v", args[len(args)-2]), false
			}
			if val == nil {
				if !validData {
					return nil, false
				}
				val = float64(math.MaxInt64)
			}
			m := val.(float64)
			if !validData {
				return m, true
			}
			switch v := args[0].(type) {
			case int:
				v1 := float64(v)
				m = getMin(m, v1)
			case int32:
				v1 := float64(v)
				m = getMin(m, v1)
			case int64:
				v1 := float64(v)
				m = getMin(m, v1)
			case float32:
				v1 := float64(v)
				m = getMin(m, v1)
			case float64:
				m = getMin(m, v)
			default:
				return fmt.Errorf("the value should be number"), false
			}
			if err := ctx.PutState(key, m); err != nil {
				return err, false
			}
			return m, true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			return ValidateLen(1, len(args))
		},
	}
	builtins["acc_sum"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			key := args[len(args)-1].(string)
			val, err := ctx.GetState(key)
			if err != nil {
				return err, false
			}
			validData, ok := args[len(args)-2].(bool)
			if !ok {
				return fmt.Errorf("when arg is not a bool but got %v", args[len(args)-2]), false
			}
			if val == nil {
				if !validData {
					return nil, false
				}
				val = float64(0)
			}
			accu := val.(float64)
			if !validData {
				return accu, true
			}
			switch sumValue := args[0].(type) {
			case int:
				accu += float64(sumValue)
			case int32:
				accu += float64(sumValue)
			case int64:
				accu += float64(sumValue)
			case float32:
				accu += float64(sumValue)
			case float64:
				accu += sumValue
			default:
				return fmt.Errorf("the value should be number"), false
			}
			if err := ctx.PutState(key, accu); err != nil {
				return err, false
			}
			return accu, true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			return ValidateLen(1, len(args))
		},
	}
	builtins["acc_count"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			key := args[len(args)-1].(string)
			val, err := ctx.GetState(key)
			if err != nil {
				return err, false
			}
			validData, ok := args[len(args)-2].(bool)
			if !ok {
				return fmt.Errorf("when arg is not a bool but got %v", args[len(args)-2]), false
			}
			if val == nil {
				if !validData {
					return nil, false
				}
				val = 0
			}
			cnt := val.(int)
			if !validData {
				return cnt, true
			}
			if args[0] != nil {
				cnt = cnt + 1
			}
			if err := ctx.PutState(key, cnt); err != nil {
				return err, false
			}
			return cnt, true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			return ValidateLen(1, len(args))
		},
	}
}

func getMax(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func getMin(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
