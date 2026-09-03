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
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
)

func registerGlobalAggFunc() {
	builtins["acc_avg"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			status, err := handleAccFunc(ctx, args, accAvgFunc{})
			if err != nil {
				return err, false
			}
			switch status.Value.(type) {
			case nil:
				return float64(0), true
			default:
				return status.Value.(*accAvgStatus).avg, true
			}
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			argsLen := len(args)
			if argsLen != 1 && argsLen != 3 {
				return fmt.Errorf("Expect 1/3 arguments but found %d.", argsLen)
			}
			return nil
		},
	}
	builtins["acc_max"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			status, err := handleAccFunc(ctx, args, accMaxFunc{})
			if err != nil {
				return err, false
			}
			switch status.Value.(type) {
			case nil:
				return float64(0), true
			default:
				return status.Value.(float64), true
			}
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			argsLen := len(args)
			if argsLen != 1 && argsLen != 3 {
				return fmt.Errorf("Expect 1/3 arguments but found %d.", argsLen)
			}
			return nil
		},
	}
	builtins["acc_min"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			status, err := handleAccFunc(ctx, args, accMinFunc{})
			if err != nil {
				return err, false
			}
			switch status.Value.(type) {
			case nil:
				return float64(0), true
			default:
				return status.Value.(float64), true
			}
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			argsLen := len(args)
			if argsLen != 1 && argsLen != 3 {
				return fmt.Errorf("Expect 1/3 arguments but found %d.", argsLen)
			}
			return nil
		},
	}
	builtins["acc_sum"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			status, err := handleAccFunc(ctx, args, accSumFunc{})
			if err != nil {
				return err, false
			}
			return status.Value.(float64), true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			argsLen := len(args)
			if argsLen != 1 && argsLen != 3 {
				return fmt.Errorf("Expect 1/3 arguments but found %d.", argsLen)
			}
			return nil
		},
	}
	builtins["acc_count"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			status, err := handleAccFunc(ctx, args, accCountFunc{})
			if err != nil {
				return err, false
			}
			return status.Value.(int64), true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			argsLen := len(args)
			if argsLen != 1 && argsLen != 3 {
				return fmt.Errorf("Expect 1/3 arguments but found %d.", argsLen)
			}
			return nil
		},
	}
	builtins["acc_collect"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			status, err := handleAccFunc(ctx, args, accCollectFunc{})
			if err != nil {
				return err, false
			}
			return status.Value.([]interface{}), true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			argsLen := len(args)
			if argsLen != 1 && argsLen != 3 {
				return fmt.Errorf("Expect 1/3 arguments but found %d.", argsLen)
			}
			return nil
		},
	}
	builtins["acc_max_by"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			status, err := handleAccMaxBy(ctx, args)
			if err != nil {
				return err, false
			}
			if status.Value == nil {
				return nil, true
			}
			return status.Value.(*accMaxByStatus).Value, true
		},
		val: func(_ api.FunctionContext, args []ast.Expr) error {
			if len(args) != 2 && len(args) != 4 {
				return fmt.Errorf("Expect 2/4 arguments but found %d.", len(args))
			}
			return nil
		},
	}
	builtins["acc_min_by"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			status, err := handleAccMinBy(ctx, args)
			if err != nil {
				return err, false
			}
			if status.Value == nil {
				return nil, true
			}
			return status.Value.(*accMinByStatus).Value, true
		},
		val: func(_ api.FunctionContext, args []ast.Expr) error {
			if len(args) != 2 && len(args) != 4 {
				return fmt.Errorf("Expect 2/4 arguments but found %d.", len(args))
			}
			return nil
		},
	}
	builtins["acc_map_agg"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			status, err := handleAccMapAgg(ctx, args)
			if err != nil {
				return err, false
			}
			return status.Value.(*accMapAggStatus).result(), true
		},
		val: func(_ api.FunctionContext, args []ast.Expr) error {
			if len(args) != 2 && len(args) != 4 {
				return fmt.Errorf("Expect 2/4 arguments but found %d.", len(args))
			}
			return nil
		},
	}
}

// accMaxByStatus keeps the latest value when compare values are equal.
type accMaxByStatus struct {
	Value interface{}
	By    float64
}

func handleAccMaxBy(ctx api.FunctionContext, args []interface{}) (*accStatus, error) {
	if len(args) != 4 && len(args) != 6 {
		return nil, fmt.Errorf("wrong args length for acc_max_by: %d", len(args))
	}
	valid, key, status, err := extractAccStatus(ctx, args)
	if err != nil {
		return nil, err
	}
	if len(args) == 6 {
		begin, ok1 := args[2].(bool)
		reset, ok2 := args[3].(bool)
		if !ok1 || !ok2 {
			return nil, fmt.Errorf("acc_max_by conditions should be boolean")
		}
		accMaxByWithCond(ctx, args[0], args[1], begin, reset, valid, key, status)
	} else {
		accMaxByExec(ctx, args[0], args[1], valid, key, status, false)
	}
	if status.Err != nil {
		return nil, status.Err
	}
	return status, nil
}

func accMaxByExec(ctx api.FunctionContext, value, by interface{}, valid bool, key string, status *accStatus, skip bool) {
	if !valid || by == nil {
		return
	}
	b, err := cast.ToFloat64(by, cast.CONVERT_SAMEKIND)
	if err != nil {
		status.Err = fmt.Errorf("acc_max_by compare_value should be number: %w", err)
		return
	}
	if math.IsNaN(b) {
		return
	}
	current, ok := status.Value.(*accMaxByStatus)
	if !ok || math.IsNaN(current.By) || b >= current.By {
		status.Value = &accMaxByStatus{Value: value, By: b}
	}
	if !skip {
		if err := ctx.PutState(key, status); err != nil {
			status.Err = err
		}
	}
}

func accMaxByWithCond(ctx api.FunctionContext, value, by interface{}, begin, reset, valid bool, key string, status *accStatus) {
	if !status.HasBegin {
		status.Value = nil
	}
	if begin && !status.HasBegin {
		status.HasBegin = true
	}
	if status.HasBegin {
		accMaxByExec(ctx, value, by, valid, key, status, true)
	}
	if reset {
		status.HasBegin = false
	}
	if status.Err == nil {
		if err := ctx.PutState(key, status); err != nil {
			status.Err = err
		}
	}
}

// accMinByStatus keeps the latest value when compare values are equal.
type accMinByStatus struct {
	Value interface{}
	By    float64
}

func handleAccMinBy(ctx api.FunctionContext, args []interface{}) (*accStatus, error) {
	if len(args) != 4 && len(args) != 6 {
		return nil, fmt.Errorf("wrong args length for acc_min_by: %d", len(args))
	}
	valid, key, status, err := extractAccStatus(ctx, args)
	if err != nil {
		return nil, err
	}
	if len(args) == 6 {
		begin, ok1 := args[2].(bool)
		reset, ok2 := args[3].(bool)
		if !ok1 || !ok2 {
			return nil, fmt.Errorf("acc_min_by conditions should be boolean")
		}
		accMinByWithCond(ctx, args[0], args[1], begin, reset, valid, key, status)
	} else {
		accMinByExec(ctx, args[0], args[1], valid, key, status, false)
	}
	if status.Err != nil {
		return nil, status.Err
	}
	return status, nil
}

func accMinByExec(ctx api.FunctionContext, value, by interface{}, valid bool, key string, status *accStatus, skip bool) {
	if !valid || by == nil {
		return
	}
	b, err := cast.ToFloat64(by, cast.CONVERT_SAMEKIND)
	if err != nil {
		status.Err = fmt.Errorf("acc_min_by compare_value should be number: %w", err)
		return
	}
	if math.IsNaN(b) {
		return
	}
	current, ok := status.Value.(*accMinByStatus)
	if !ok || math.IsNaN(current.By) || b <= current.By {
		status.Value = &accMinByStatus{Value: value, By: b}
	}
	if !skip {
		if err := ctx.PutState(key, status); err != nil {
			status.Err = err
		}
	}
}

func accMinByWithCond(ctx api.FunctionContext, value, by interface{}, begin, reset, valid bool, key string, status *accStatus) {
	if !status.HasBegin {
		status.Value = nil
	}
	if begin && !status.HasBegin {
		status.HasBegin = true
	}
	if status.HasBegin {
		accMinByExec(ctx, value, by, valid, key, status, true)
	}
	if reset {
		status.HasBegin = false
	}
	if status.Err == nil {
		if err := ctx.PutState(key, status); err != nil {
			status.Err = err
		}
	}
}

type accMapEntry struct {
	Key   string
	Value interface{}
}
type accMapAggStatus struct {
	Entries []accMapEntry
	Index   map[string]int
}

func newAccMapAggStatus() *accMapAggStatus {
	return &accMapAggStatus{Index: make(map[string]int)}
}

func (s *accMapAggStatus) result() []map[string]interface{} {
	r := make([]map[string]interface{}, 0, len(s.Entries))
	for _, e := range s.Entries {
		r = append(r, map[string]interface{}{kvPairKName: e.Key, kvPairVName: e.Value})
	}
	return r
}

func handleAccMapAgg(ctx api.FunctionContext, args []interface{}) (*accStatus, error) {
	if len(args) != 4 && len(args) != 6 {
		return nil, fmt.Errorf("wrong args length for acc_map_agg: %d", len(args))
	}
	valid, key, status, err := extractAccStatus(ctx, args)
	if err != nil {
		return nil, err
	}
	if _, ok := status.Value.(*accMapAggStatus); !ok {
		status.Value = newAccMapAggStatus()
	}
	if len(args) == 6 {
		begin, ok1 := args[2].(bool)
		reset, ok2 := args[3].(bool)
		if !ok1 || !ok2 {
			return nil, fmt.Errorf("acc_map_agg conditions should be boolean")
		}
		if !status.HasBegin {
			status.Value = newAccMapAggStatus()
		}
		if begin && !status.HasBegin {
			status.HasBegin = true
		}
		if status.HasBegin {
			accMapAggExec(ctx, args[0], args[1], valid, key, status)
		}
		if reset {
			status.HasBegin = false
		}
	} else {
		accMapAggExec(ctx, args[0], args[1], valid, key, status)
	}
	if status.Err != nil {
		return nil, status.Err
	}
	if err := ctx.PutState(key, status); err != nil {
		return nil, err
	}
	return status, nil
}

func accMapAggExec(ctx api.FunctionContext, k, value interface{}, valid bool, stateKey string, status *accStatus) {
	if !valid || k == nil {
		return
	}
	ks, err := cast.ToString(k, cast.CONVERT_ALL)
	if err != nil {
		status.Err = fmt.Errorf("acc_map_agg key should be string-convertible: %w", err)
		return
	}
	s := status.Value.(*accMapAggStatus)
	if s.Index == nil {
		s.Index = make(map[string]int, len(s.Entries))
		for i, entry := range s.Entries {
			s.Index[entry.Key] = i
		}
	}
	if i, ok := s.Index[ks]; ok {
		s.Entries[i].Value = value
		return
	}
	s.Index[ks] = len(s.Entries)
	s.Entries = append(s.Entries, accMapEntry{Key: ks, Value: value})
}

func handleAccFunc(ctx api.FunctionContext, args []interface{}, accFunc accFunc) (*accStatus, error) {
	validData, partitionKey, status, err := extractAccArgs(ctx, args, accFunc)
	if err != nil {
		return nil, err
	}
	if len(args) == 3 {
		accFunc.accFuncExec(ctx, args[0], validData, partitionKey, status, false)
		if status.Err != nil {
			return nil, status.Err
		}
		return status, nil
	}
	if len(args) == 5 {
		if err := handleOnCondAccFunc(ctx, args, validData, partitionKey, status, accFunc); err != nil {
			return nil, err
		}
		if status.Err != nil {
			return nil, status.Err
		}
		return status, nil
	}
	return nil, fmt.Errorf("wrong args length")
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
	validData, partitionKey, status, err = extractAccStatus(ctx, args)
	if err != nil {
		return false, "", nil, err
	}
	if status.Value == nil {
		accFunc.accReset(status)
	}
	return validData, partitionKey, status, nil
}

func extractAccStatus(ctx api.FunctionContext, args []interface{}) (validData bool, partitionKey string, status *accStatus, err error) {
	partitionKey, ok := args[len(args)-1].(string)
	if !ok {
		return false, "", nil, fmt.Errorf("invalid state key")
	}
	validData, ok = args[len(args)-2].(bool)
	if !ok {
		return false, "", nil, fmt.Errorf("valid data should be boolean")
	}
	val, err := ctx.GetState(partitionKey)
	if err != nil {
		return false, "", nil, err
	}
	if val == nil {
		val = &accStatus{}
	}
	status, ok = val.(*accStatus)
	if !ok {
		return false, "", nil, fmt.Errorf("invalid accumulator state type %T", val)
	}
	status.Err = nil
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
	if onBegin && !status.HasBegin {
		status.HasBegin = true
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
		status.Value = int64(0)
	}
	cnt := status.Value.(int64)
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
	status.Value = int64(0)
}

type accSumFunc struct{}

func (a accSumFunc) accFuncExec(ctx api.FunctionContext, value interface{}, validData bool, partitionKey string, status *accStatus, skipStatusSave bool) {
	if status.Value == nil {
		status.Value = float64(0)
	}
	sum := status.Value.(float64)
	if !validData {
		return
	}
	if value != nil {
		switch v := value.(type) {
		// only for unit test
		case int:
			sum += float64(v)
			status.Value = sum
		case int64:
			sum += float64(v)
			status.Value = sum
		case float64:
			sum += v
			status.Value = sum
		default:
			status.Err = fmt.Errorf("the value should be number")
		}
	}
	if !skipStatusSave {
		if err := ctx.PutState(partitionKey, status); err != nil {
			status.Err = err
		}
	}
}

func (a accSumFunc) accReset(status *accStatus) {
	status.Value = float64(0)
}

type accMinFunc struct{}

func (a accMinFunc) accFuncExec(ctx api.FunctionContext, value interface{}, validData bool, partitionKey string, status *accStatus, skipStatusSave bool) {
	if !validData {
		return
	}
	mv := float64(math.MaxInt64)
	sfv, ok := status.Value.(float64)
	if ok {
		mv = sfv
	}
	switch v := value.(type) {
	case int:
		mv = getMin(mv, float64(v))
		status.Value = mv
	case int64:
		mv = getMin(mv, float64(v))
		status.Value = mv
	case float64:
		mv = getMin(mv, v)
		status.Value = mv
	default:
		status.Err = fmt.Errorf("the value should be number")
	}
	if !skipStatusSave {
		if err := ctx.PutState(partitionKey, status); err != nil {
			status.Err = err
		}
	}
}

func (a accMinFunc) accReset(status *accStatus) {
	status.Value = nil
}

type accMaxFunc struct{}

func (a accMaxFunc) accFuncExec(ctx api.FunctionContext, value interface{}, validData bool, partitionKey string, status *accStatus, skipStatusSave bool) {
	if !validData {
		return
	}
	mv := float64(math.MinInt64)
	sfv, ok := status.Value.(float64)
	if ok {
		mv = sfv
	}
	switch v := value.(type) {
	case int:
		mv = getMax(mv, float64(v))
		status.Value = mv
	case int64:
		mv = getMax(mv, float64(v))
		status.Value = mv
	case float64:
		mv = getMax(mv, v)
		status.Value = mv
	default:
		status.Err = fmt.Errorf("the value should be number")
	}
	if !skipStatusSave {
		if err := ctx.PutState(partitionKey, status); err != nil {
			status.Err = err
		}
	}
}

func (a accMaxFunc) accReset(status *accStatus) {
	status.Value = nil
}

type accAvgFunc struct{}

func (a accAvgFunc) accFuncExec(ctx api.FunctionContext, value interface{}, validData bool, partitionKey string, status *accStatus, skipStatusSave bool) {
	if !validData {
		return
	}
	avgStatus := &accAvgStatus{}
	sas, ok := status.Value.(*accAvgStatus)
	if ok {
		avgStatus = sas
	}
	switch v := value.(type) {
	case int:
		avgStatus.sum += float64(v)
		avgStatus.count++
		avgStatus.avg = avgStatus.sum / float64(avgStatus.count)
		status.Value = avgStatus
	case int64:
		avgStatus.sum += float64(v)
		avgStatus.count++
		avgStatus.avg = avgStatus.sum / float64(avgStatus.count)
		status.Value = avgStatus
	case float64:
		avgStatus.sum += v
		avgStatus.count++
		avgStatus.avg = avgStatus.sum / float64(avgStatus.count)
		status.Value = avgStatus
	default:
		status.Err = fmt.Errorf("the value should be number")
	}
	if !skipStatusSave {
		if err := ctx.PutState(partitionKey, status); err != nil {
			status.Err = err
		}
	}
}

func (a accAvgFunc) accReset(status *accStatus) {
	status.Value = nil
}

type accCollectFunc struct{}

func (a accCollectFunc) accFuncExec(ctx api.FunctionContext, value interface{}, validData bool, partitionKey string, status *accStatus, skipStatusSave bool) {
	if status.Value == nil {
		status.Value = []interface{}{}
	}
	collected := status.Value.([]interface{})
	if !validData {
		return
	}
	if value != nil {
		collected = append(collected, value)
		status.Value = collected
	}
	if !skipStatusSave {
		if err := ctx.PutState(partitionKey, status); err != nil {
			status.Err = err
		}
	}
}

func (a accCollectFunc) accReset(status *accStatus) {
	status.Value = []interface{}{}
}

type accAvgStatus struct {
	sum   float64
	count int64
	avg   float64
}
