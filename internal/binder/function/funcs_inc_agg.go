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

	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
)

var supportedIncAggFunc = map[string]struct{}{
	"count": {},
	"avg":   {},
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
}

func incrementalCount(ctx api.FunctionContext, arg interface{}) (int64, error) {
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
