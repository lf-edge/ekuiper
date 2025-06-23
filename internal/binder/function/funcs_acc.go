// Copyright 2025 EMQ Technologies Co., Ltd.
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

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

func registerGlobalAggFunc() {
	builtins["acc_avg"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			key := args[len(args)-1].(string)
			keyCount := fmt.Sprintf("%s_count", key)
			keySum := fmt.Sprintf("%s_sum", key)
			keyAvg := fmt.Sprintf("%s_avg", key)

			vCount, err := ctx.GetState(keyCount)
			if err != nil {
				return err, false
			}
			vSum, err := ctx.GetState(keySum)
			if err != nil {
				return err, false
			}
			vAvg, err := ctx.GetState(keyAvg)
			if err != nil {
				return err, false
			}
			validData, ok := args[len(args)-2].(bool)
			if !ok {
				return fmt.Errorf("when arg is not a bool but got %v", args[len(args)-2]), false
			}
			if vSum == nil || vCount == nil || vAvg == nil {
				vSum = float64(0)
				vCount = float64(0)
				vAvg = float64(0)
			}
			if args[0] == nil || !validData {
				return vAvg.(float64), true
			}
			count := vCount.(float64)
			sum := vSum.(float64)
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
			if err := ctx.PutState(keyAvg, sum/count); err != nil {
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
					return 0, true
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
					return 0, true
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
			accCntFunc := accCountFunc{}
			validData, partitionKey, status, err := extractAccArgs(ctx, args, accCntFunc)
			if err != nil {
				return err, false
			}
			if len(args) == 3 {
				accCntFunc.accFuncExec(ctx, args[0], validData, partitionKey, status, false)
				if status.Err != nil {
					return status.Err, false
				}
				return status.Value.(int), true
			}
			if len(args) == 5 {
				if err := handleOnCondAccFunc(ctx, args, validData, partitionKey, status, accCntFunc); err != nil {
					return err, false
				}
				if status.Err != nil {
					return status.Err, false
				}
				return status.Value.(int), true
			}
			return fmt.Errorf("wrong args length"), false
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			argsLen := len(args)
			if argsLen != 1 && argsLen != 3 {
				return fmt.Errorf("Expect 1/3 arguments but found %d.", argsLen)
			}
			return nil
		},
	}
}

func handleOnCondAccFunc(ctx api.FunctionContext, args []interface{}, validData bool, partitionKey string, status *accStatus, accFunc accFunc) error {
	onBegin, ok := args[1].(bool)
	if !ok {
		return fmt.Errorf("onBegin should be boolean")
	}
	onReset, ok := args[2].(bool)
	if !ok {
		return fmt.Errorf("onReset should be boolean")
	}
	accFuncWithCond(ctx, args[0], onBegin, onReset, validData, partitionKey, status, accFunc)
	return nil
}

func extractAccArgs(ctx api.FunctionContext, args []interface{}, accFunc accFunc) (validData bool, partitionKey string, status *accStatus, err error) {
	partitionKey = args[len(args)-1].(string)
	validData = args[len(args)-2].(bool)
	val, err := ctx.GetState(partitionKey)
	if err != nil {
		return false, "", nil, err
	}
	if val == nil {
		val = &accStatus{}
	}
	status = val.(*accStatus)
	status.Err = nil
	if status.Value == nil {
		accFunc.accReset(status)
	}
	return validData, partitionKey, status, nil
}

// accFuncWithCond execute acc function with onBegin and onReset condition with following 4 steps:
// 1. Check HasBegin at the beginning, if it's false, it means any result won't be calculated, thus we need to always reset the value
// 2. Check onBegin condition to set the HasBegin
// 3. Check HasBegin to determine whether calculate the acc function
// 4. Check onReset to set the HasBegin
func accFuncWithCond(ctx api.FunctionContext, value interface{}, onBegin, onReset bool, validData bool, partitionKey string, status *accStatus, accFunc accFunc) {
	if !status.HasBegin {
		accFunc.accReset(status)
	}
	if onBegin {
		if !status.HasBegin {
			accFunc.accReset(status)
			status.HasBegin = true
		}
	}
	if status.HasBegin {
		accFunc.accFuncExec(ctx, value, validData, partitionKey, status, true)
		if status.Err != nil {
			return
		}
	}
	if onReset {
		if status.HasBegin {
			status.HasBegin = false
		}
	}
	if err := ctx.PutState(partitionKey, status); err != nil {
		status.Err = err
		return
	}
}

type accStatus struct {
	Err      error
	Value    interface{}
	HasBegin bool
}

type accFunc interface {
	accFuncExec(ctx api.FunctionContext, value interface{}, validData bool, partitionKey string, status *accStatus, skipStatusSave bool)
	accReset(status *accStatus)
}

type accCountFunc struct{}

func (a accCountFunc) accFuncExec(ctx api.FunctionContext, value interface{}, validData bool, partitionKey string, status *accStatus, skipStatusSave bool) {
	if status.Value == nil {
		status.Value = 0
	}
	cnt := status.Value.(int)
	if !validData {
		return
	}
	if value != nil {
		cnt++
		status.Value = cnt
	}
	if !skipStatusSave {
		if err := ctx.PutState(partitionKey, status); err != nil {
			status.Err = err
		}
	}
}

func (a accCountFunc) accReset(status *accStatus) {
	status.Value = 0
}
