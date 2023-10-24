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

	"github.com/montanaflynn/stats"

	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

func registerAggFunc() {
	builtins["avg"] = builtinFunc{
		fType: ast.FuncTypeAgg,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0 := args[0].([]interface{})
			c := getCount(arg0)
			if c > 0 {
				v := getFirstValidArg(arg0)
				switch v.(type) {
				case int, int64:
					if r, err := sliceIntTotal(arg0); err != nil {
						return err, false
					} else {
						return r / int64(c), true
					}
				case float64:
					if r, err := sliceFloatTotal(arg0); err != nil {
						return err, false
					} else {
						return r / float64(c), true
					}
				case nil:
					return nil, true
				default:
					return fmt.Errorf("run avg function error: found invalid arg %[1]T(%[1]v)", v), false
				}
			}
			return nil, true
		},
		val:   ValidateOneNumberArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["count"] = builtinFunc{
		fType: ast.FuncTypeAgg,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0 := args[0].([]interface{})
			return getCount(arg0), true
		},
		val:   ValidateOneArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["max"] = builtinFunc{
		fType: ast.FuncTypeAgg,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0 := args[0].([]interface{})
			return max(arg0)
		},
		val:   ValidateOneNumberArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["min"] = builtinFunc{
		fType: ast.FuncTypeAgg,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0 := args[0].([]interface{})
			return min(arg0)
		},
		val:   ValidateOneNumberArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["sum"] = builtinFunc{
		fType: ast.FuncTypeAgg,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0 := args[0].([]interface{})
			if len(arg0) > 0 {
				v := getFirstValidArg(arg0)
				switch v.(type) {
				case int, int64:
					if r, err := sliceIntTotal(arg0); err != nil {
						return err, false
					} else {
						return r, true
					}
				case float64:
					if r, err := sliceFloatTotal(arg0); err != nil {
						return err, false
					} else {
						return r, true
					}
				case nil:
					return nil, true
				default:
					return fmt.Errorf("run sum function error: found invalid arg %[1]T(%[1]v)", v), false
				}
			}
			return nil, true
		},
		val:   ValidateOneNumberArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["collect"] = builtinFunc{
		fType: ast.FuncTypeAgg,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if len(args) > 0 {
				return args[0], true
			}
			return make([]interface{}, 0), true
		},
		val: ValidateOneArg,
	}
	builtins["merge_agg"] = builtinFunc{
		fType: ast.FuncTypeAgg,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			data, ok := args[0].([]interface{})
			if ok {
				result := make(map[string]interface{})
				for _, ele := range data {
					if m, ok := ele.(map[string]interface{}); ok {
						for k, v := range m {
							result[k] = v
						}
					}
				}
				if len(result) > 0 {
					return result, true
				}
			}
			return nil, true
		},
		val:   ValidateOneArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["deduplicate"] = builtinFunc{
		fType: ast.FuncTypeAgg,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			v1, ok1 := args[0].([]interface{})
			v2, ok2 := args[1].([]interface{})
			v3a, ok3 := args[2].([]interface{})

			if ok1 && ok2 && ok3 && len(v3a) > 0 {
				v3, ok4 := getFirstValidArg(v3a).(bool)
				if ok4 {
					if r, err := dedup(v1, v2, v3); err != nil {
						return err, false
					} else {
						return r, true
					}
				}
			}
			return fmt.Errorf("Invalid argument type found."), false
		},
		val: func(_ api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(2, len(args)); err != nil {
				return err
			}
			if !ast.IsBooleanArg(args[1]) {
				return ProduceErrInfo(1, "bool")
			}
			return nil
		},
		check: returnNilIfHasAnyNil,
	}
	builtins["stddev"] = builtinFunc{
		fType: ast.FuncTypeAgg,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0 := args[0].([]interface{})
			if len(arg0) > 0 {
				float64Slice, err := cast.ToFloat64Slice(arg0, cast.CONVERT_SAMEKIND)
				if err != nil {
					return fmt.Errorf("requires float64 slice but found %[1]T(%[1]v)", arg0), false
				}
				deviation, err := stats.StandardDeviation(float64Slice)
				if err != nil {
					if err == stats.EmptyInputErr {
						return nil, true
					}
					return fmt.Errorf("StandardDeviation exec with error: %v", err), false
				}
				return deviation, true
			}
			return nil, true
		},
		val:   ValidateOneNumberArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["stddevs"] = builtinFunc{
		fType: ast.FuncTypeAgg,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0 := args[0].([]interface{})
			if len(arg0) > 0 {
				float64Slice, err := cast.ToFloat64Slice(arg0, cast.CONVERT_SAMEKIND)
				if err != nil {
					return fmt.Errorf("requires float64 slice but found %[1]T(%[1]v)", arg0), false
				}
				deviation, err := stats.StandardDeviationSample(float64Slice)
				if err != nil {
					if err == stats.EmptyInputErr {
						return nil, true
					}
					return fmt.Errorf("StandardDeviationSample exec with error: %v", err), false
				}
				return deviation, true
			}
			return nil, true
		},
		val:   ValidateOneNumberArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["var"] = builtinFunc{
		fType: ast.FuncTypeAgg,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0 := args[0].([]interface{})
			if len(arg0) > 0 {
				float64Slice, err := cast.ToFloat64Slice(arg0, cast.CONVERT_SAMEKIND)
				if err != nil {
					return fmt.Errorf("requires float64 slice but found %[1]T(%[1]v)", arg0), false
				}
				deviation, err := stats.Variance(float64Slice)
				if err != nil {
					if err == stats.EmptyInputErr {
						return nil, true
					}
					return fmt.Errorf("PopulationVariance exec with error: %v", err), false
				}
				return deviation, true
			}
			return nil, true
		},
		val:   ValidateOneNumberArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["vars"] = builtinFunc{
		fType: ast.FuncTypeAgg,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0 := args[0].([]interface{})
			if len(arg0) > 0 {
				float64Slice, err := cast.ToFloat64Slice(arg0, cast.CONVERT_SAMEKIND)
				if err != nil {
					return fmt.Errorf("requires float64 slice but found %[1]T(%[1]v)", arg0), false
				}
				deviation, err := stats.SampleVariance(float64Slice)
				if err != nil {
					if err == stats.EmptyInputErr {
						return nil, true
					}
					return fmt.Errorf("SampleVariance exec with error: %v", err), false
				}
				return deviation, true
			}
			return nil, true
		},
		val:   ValidateOneNumberArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["percentile_cont"] = builtinFunc{
		fType: ast.FuncTypeAgg,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if err := ValidateLen(2, len(args)); err != nil {
				return err, false
			}
			var arg1Float64 float64 = 1
			arg0 := args[0].([]interface{})
			arg1 := args[1].([]interface{})
			if len(arg1) > 0 {
				v1 := getFirstValidArg(arg1)
				val, err := cast.ToFloat64(v1, cast.CONVERT_SAMEKIND)
				if err != nil {
					return fmt.Errorf("the second parameter requires float64 but found %[1]T(%[1]v)", arg1), false
				}
				arg1Float64 = val
			}

			if len(arg0) > 0 {
				float64Slice, err := cast.ToFloat64Slice(arg0, cast.CONVERT_SAMEKIND)
				if err != nil {
					return fmt.Errorf("requires float64 slice but found %[1]T(%[1]v)", arg0), false
				}
				deviation, err := stats.Percentile(float64Slice, arg1Float64*100)
				if err != nil {
					if err == stats.EmptyInputErr {
						return nil, true
					}
					return fmt.Errorf("percentile exec with error: %v", err), false
				}
				return deviation, true
			}
			return nil, true
		},
		val:   ValidateTwoNumberArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["percentile_disc"] = builtinFunc{
		fType: ast.FuncTypeAgg,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if err := ValidateLen(2, len(args)); err != nil {
				return err, false
			}
			var arg1Float64 float64 = 1
			arg0 := args[0].([]interface{})
			arg1 := args[1].([]interface{})
			if len(arg1) > 0 {
				v1 := getFirstValidArg(arg1)
				val, err := cast.ToFloat64(v1, cast.CONVERT_SAMEKIND)
				if err != nil {
					return fmt.Errorf("the second parameter requires float64 but found %[1]T(%[1]v)", arg1), false
				}
				arg1Float64 = val
			}
			if len(arg0) > 0 {
				float64Slice, err := cast.ToFloat64Slice(arg0, cast.CONVERT_SAMEKIND)
				if err != nil {
					return fmt.Errorf("requires float64 slice but found %[1]T(%[1]v)", arg0), false
				}
				deviation, err := stats.PercentileNearestRank(float64Slice, arg1Float64*100)
				if err != nil {
					if err == stats.EmptyInputErr {
						return nil, true
					}
					return fmt.Errorf("PopulationVariance exec with error: %v", err), false
				}
				return deviation, true
			}
			return nil, true
		},
		val:   ValidateTwoNumberArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["last_value"] = builtinFunc{
		fType: ast.FuncTypeAgg,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0, ok := args[0].([]interface{})
			if !ok {
				return fmt.Errorf("the first argument to the aggregate function should be []interface but found %[1]T(%[1]v)", args[0]), false
			}
			args1, ok := args[1].([]interface{})
			if !ok {
				return fmt.Errorf("the second argument to the aggregate function should be []interface but found %[1]T(%[1]v)", args[1]), false
			}
			arg1, ok := getFirstValidArg(args1).(bool)
			if !ok {
				return fmt.Errorf("the second parameter requires bool but found %[1]T(%[1]v)", getFirstValidArg(args1)), false
			}
			if len(arg0) == 0 {
				return nil, true
			}
			if arg1 {
				for i := len(arg0) - 1; i >= 0; i-- {
					if arg0[i] != nil {
						return arg0[i], true
					}
				}
			}
			return arg0[len(arg0)-1], true
		},
		val: func(_ api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(2, len(args)); err != nil {
				return err
			}
			if !ast.IsBooleanArg(args[1]) {
				return ProduceErrInfo(1, "bool")
			}
			return nil
		},
		check: returnNilIfHasAnyNil,
	}
}
