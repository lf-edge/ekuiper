// Copyright 2024 EMQ Technologies Co., Ltd.
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

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/pingcap/failpoint"

	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
)

var supportedIncAggFunc = map[string]struct{}{
	"count":      {},
	"avg":        {},
	"max":        {},
	"min":        {},
	"sum":        {},
	"merge_agg":  {},
	"collect":    {},
	"last_value": {},
}

func IsSupportedIncAgg(name string) bool {
	_, ok := supportedIncAggFunc[name]
	return ok
}

func registerIncAggFunc() {
	builtins["inc_count"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			c, err := incrementalCount(ctx, args[0])
			if err != nil {
				return err, false
			}
			return c, true
		},
		val:   ValidateOneArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["inc_avg"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0, err := cast.ToFloat64(args[0], cast.CONVERT_ALL)
			if err != nil {
				return err, false
			}
			count, err := incrementalCount(ctx, arg0)
			if err != nil {
				return err, false
			}
			sum, err := incrementalSum(ctx, arg0)
			if err != nil {
				return err, false
			}
			return sum / float64(count), true
		},
		val:   ValidateOneNumberArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["inc_max"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0 := args[0]
			result, err := incrementalMax(ctx, arg0)
			if err != nil {
				return err, false
			}
			return result, true
		},
		val:   ValidateOneNumberArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["inc_min"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0 := args[0]
			result, err := incrementalMin(ctx, arg0)
			if err != nil {
				return err, false
			}
			return result, true
		},
		val:   ValidateOneNumberArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["inc_sum"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0, err := cast.ToFloat64(args[0], cast.CONVERT_ALL)
			if err != nil {
				return err, false
			}
			result, err := incrementalSum(ctx, arg0)
			if err != nil {
				return err, false
			}
			return result, true
		},
		val:   ValidateOneNumberArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["inc_merge_agg"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0, ok := args[0].(map[string]interface{})
			if !ok {
				return fmt.Errorf("argument is not a map[string]interface{}"), false
			}
			result, err := incrementalMerge(ctx, arg0)
			if err != nil {
				return err, false
			}
			return result, true
		},
		val:   ValidateOneNumberArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["inc_collect"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0 := args[0]
			result, err := incrementalCollect(ctx, arg0)
			if err != nil {
				return err, false
			}
			return result, true
		},
		val:   ValidateOneNumberArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["inc_last_value"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0 := args[0]
			arg1, ok := args[1].(bool)
			if !ok {
				return fmt.Errorf("second argument is not a bool"), false
			}
			result, err := incrementalLastValue(ctx, arg0, arg1)
			if err != nil {
				return err, false
			}
			return result, true
		},
		val:   ValidateTwoNumberArg,
		check: returnNilIfHasAnyNil,
	}
}

func incrementalLastValue(ctx api.FunctionContext, arg interface{}, ignoreNil bool) (interface{}, error) {
	failpoint.Inject("inc_err", func() {
		failpoint.Return(nil, fmt.Errorf("inc err"))
	})
	key := fmt.Sprintf("%v_inc_last_value", ctx.GetFuncId())
	v, err := ctx.GetState(key)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		if !ignoreNil {
			return nil, nil
		} else {
			return v, nil
		}
	} else {
		ctx.PutState(key, arg)
		return arg, nil
	}
}

func incrementalCollect(ctx api.FunctionContext, arg interface{}) ([]interface{}, error) {
	failpoint.Inject("inc_err", func() {
		failpoint.Return(nil, fmt.Errorf("inc err"))
	})
	key := fmt.Sprintf("%v_inc_collect", ctx.GetFuncId())
	var listV []interface{}
	v, err := ctx.GetState(key)
	if err != nil {
		return nil, err
	}
	if v == nil {
		listV = make([]interface{}, 0)
	} else {
		llv, ok := v.([]interface{})
		if ok {
			listV = llv
		}
	}
	listV = append(listV, arg)
	ctx.PutState(key, listV)
	return listV, nil
}

func incrementalMerge(ctx api.FunctionContext, arg map[string]interface{}) (map[string]interface{}, error) {
	failpoint.Inject("inc_err", func() {
		failpoint.Return(nil, fmt.Errorf("inc err"))
	})
	key := fmt.Sprintf("%v_inc_merge_agg", ctx.GetFuncId())
	var mv map[string]interface{}
	v, err := ctx.GetState(key)
	if err != nil {
		return nil, err
	}
	if v == nil {
		mv = make(map[string]interface{})
	} else {
		mmv, ok := v.(map[string]interface{})
		if ok {
			mv = mmv
		}
	}
	for k, value := range arg {
		mv[k] = value
	}
	ctx.PutState(key, mv)
	return mv, nil
}

func incrementalMin(ctx api.FunctionContext, arg interface{}) (interface{}, error) {
	failpoint.Inject("inc_err", func() {
		failpoint.Return(nil, fmt.Errorf("inc err"))
	})
	key := fmt.Sprintf("%v_inc_min", ctx.GetFuncId())
	v, err := ctx.GetState(key)
	if err != nil {
		return nil, err
	}
	args := make([]interface{}, 0)
	args = append(args, arg)
	if v != nil {
		args = append(args, v)
	}
	result, _ := min(args)
	switch result.(type) {
	case error:
		return nil, err
	case int64, float64, string:
		ctx.PutState(key, result)
		return result, nil
	case nil:
		return nil, nil
	}
	return nil, nil
}

func incrementalMax(ctx api.FunctionContext, arg interface{}) (interface{}, error) {
	failpoint.Inject("inc_err", func() {
		failpoint.Return(nil, fmt.Errorf("inc err"))
	})
	key := fmt.Sprintf("%v_inc_max", ctx.GetFuncId())
	v, err := ctx.GetState(key)
	if err != nil {
		return nil, err
	}
	args := make([]interface{}, 0)
	args = append(args, arg)
	if v != nil {
		args = append(args, v)
	}
	result, _ := max(args)
	switch result.(type) {
	case error:
		return nil, err
	case int64, float64, string:
		ctx.PutState(key, result)
		return result, nil
	case nil:
		return nil, nil
	}
	return nil, nil
}

func incrementalCount(ctx api.FunctionContext, arg interface{}) (int64, error) {
	failpoint.Inject("inc_err", func() {
		failpoint.Return(0, fmt.Errorf("inc err"))
	})
	key := fmt.Sprintf("%v_inc_count", ctx.GetFuncId())
	v, err := ctx.GetState(key)
	if err != nil {
		return 0, err
	}
	var c int64
	if v == nil {
		c = 1
	} else {
		c = v.(int64) + 1
	}
	ctx.PutState(key, c)
	return c, nil
}

func incrementalSum(ctx api.FunctionContext, arg float64) (float64, error) {
	failpoint.Inject("inc_err", func() {
		failpoint.Return(0, fmt.Errorf("inc err"))
	})
	key := fmt.Sprintf("%v_inc_sum", ctx.GetFuncId())
	v, err := ctx.GetState(key)
	if err != nil {
		return 0, err
	}
	var sum float64
	if v == nil {
		sum = arg
	} else {
		sum = v.(float64) + arg
	}
	ctx.PutState(key, sum)
	return sum, nil
}
