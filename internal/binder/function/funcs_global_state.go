// Copyright 2023-2024 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

const (
	countKey    = "$$last_hit_count"
	timeKey     = "$$last_hit_time"
	aggCountKey = "$$last_agg_hit_count"
	aggTimeKey  = "$$last_agg_hit_time"
)

func registerGlobalStateFunc() {
	builtins["last_hit_count"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			doUpdate := args[0].(bool)
			lv, err := ctx.GetCounter(countKey)
			if err != nil {
				return err, false
			}
			if doUpdate {
				err := ctx.IncrCounter(countKey, 1)
				if err != nil {
					return nil, false
				}
			}
			return lv, true
		},
		val: ValidateNoArg,
	}
	builtins["last_hit_time"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			args0 := args[0].(bool)
			args1 := args[1].(int64)

			lv, err := ctx.GetState(timeKey)
			if err != nil {
				return err, false
			}
			if lv == nil {
				lv = 0
			}
			if args0 {
				err := ctx.PutState(timeKey, args1)
				if err != nil {
					return nil, false
				}
			}
			return lv, true
		},
		val: ValidateNoArg,
	}
	builtins["last_agg_hit_count"] = builtinFunc{
		fType: ast.FuncTypeAgg,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			doUpdate := args[0].(bool)
			lv, err := ctx.GetCounter(aggCountKey)
			if err != nil {
				return err, false
			}
			if doUpdate {
				err := ctx.IncrCounter(aggCountKey, 1)
				if err != nil {
					return nil, false
				}
			}
			return lv, true
		},
		val: ValidateNoArg,
	}
	builtins["last_agg_hit_time"] = builtinFunc{
		fType: ast.FuncTypeAgg,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			args0 := args[0].(bool)
			args1 := args[1].(int64)

			lv, err := ctx.GetState(aggTimeKey)
			if err != nil {
				return err, false
			}
			if lv == nil {
				lv = 0
			}
			if args0 {
				err := ctx.PutState(aggTimeKey, args1)
				if err != nil {
					return nil, false
				}
			}
			return lv, true
		},
		val: ValidateNoArg,
	}
}
