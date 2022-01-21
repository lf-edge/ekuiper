// Copyright 2022 EMQ Technologies Co., Ltd.
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
	"math"
	"math/rand"
)

func registerMathFunc() {
	builtins["abs"] = builtinFunc{
		fType: FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, ok := args[0].(int); ok {
				t := float64(v)
				var ret = int(math.Abs(t))
				return ret, true
			} else if v, ok := args[0].(float64); ok {
				return math.Abs(v), true
			} else {
				return fmt.Errorf("only float64 & int type are supported"), false
			}
		},
		val: ValidateOneNumberArg,
	}
	builtins["acos"] = builtinFunc{
		fType: FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := toF64(args[0]); e == nil {
				return math.Acos(v), true
			} else {
				return e, false
			}
		},
		val: ValidateOneNumberArg,
	}
	builtins["asin"] = builtinFunc{
		fType: FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := toF64(args[0]); e == nil {
				return math.Asin(v), true
			} else {
				return e, false
			}
		},
		val: ValidateOneNumberArg,
	}
	builtins["atan"] = builtinFunc{
		fType: FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := toF64(args[0]); e == nil {
				return math.Atan(v), true
			} else {
				return e, false
			}
		},
		val: ValidateOneNumberArg,
	}
	builtins["atan2"] = builtinFunc{
		fType: FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v1, e := toF64(args[0]); e == nil {
				if v2, e1 := toF64(args[1]); e1 == nil {
					return math.Atan2(v1, v2), true
				} else {
					return e1, false
				}
			} else {
				return e, false
			}
		},
		val: ValidateTwoNumberArg,
	}
	builtins["bitand"] = builtinFunc{
		fType: FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			v1, ok1 := args[0].(int)
			v2, ok2 := args[1].(int)
			if ok1 && ok2 {
				return v1 & v2, true
			} else {
				return fmt.Errorf("Expect int type for both operands."), false
			}
		},
		val: ValidateTwoIntArg,
	}
	builtins["bitor"] = builtinFunc{
		fType: FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			v1, ok1 := args[0].(int)
			v2, ok2 := args[1].(int)
			if ok1 && ok2 {
				return v1 | v2, true
			} else {
				return fmt.Errorf("Expect int type for both operands."), false
			}
		},
		val: ValidateTwoIntArg,
	}
	builtins["bitxor"] = builtinFunc{
		fType: FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			v1, ok1 := args[0].(int)
			v2, ok2 := args[1].(int)
			if ok1 && ok2 {
				return v1 ^ v2, true
			} else {
				return fmt.Errorf("Expect int type for both operands."), false
			}
		},
		val: ValidateTwoIntArg,
	}
	builtins["bitnot"] = builtinFunc{
		fType: FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			v1, ok1 := args[0].(int)
			if ok1 {
				return ^v1, true
			} else {
				return fmt.Errorf("Expect int type for operand."), false
			}
		},
		val: func(_ api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(1, len(args)); err != nil {
				return err
			}
			if ast.IsFloatArg(args[0]) || ast.IsStringArg(args[0]) || ast.IsTimeArg(args[0]) || ast.IsBooleanArg(args[0]) {
				return ProduceErrInfo(0, "int")
			}
			return nil
		},
	}
	builtins["ceil"] = builtinFunc{
		fType: FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := toF64(args[0]); e == nil {
				return math.Ceil(v), true
			} else {
				return e, false
			}
		},
		val: ValidateOneNumberArg,
	}
	builtins["cos"] = builtinFunc{
		fType: FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := toF64(args[0]); e == nil {
				return math.Cos(v), true
			} else {
				return e, false
			}
		},
		val: ValidateOneNumberArg,
	}
	builtins["cosh"] = builtinFunc{
		fType: FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := toF64(args[0]); e == nil {
				return math.Cosh(v), true
			} else {
				return e, false
			}
		},
		val: ValidateOneNumberArg,
	}
	builtins["exp"] = builtinFunc{
		fType: FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := toF64(args[0]); e == nil {
				return math.Exp(v), true
			} else {
				return e, false
			}
		},
		val: ValidateOneNumberArg,
	}
	builtins["ln"] = builtinFunc{
		fType: FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := toF64(args[0]); e == nil {
				return math.Log2(v), true
			} else {
				return e, false
			}
		},
		val: ValidateOneNumberArg,
	}
	builtins["log"] = builtinFunc{
		fType: FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := toF64(args[0]); e == nil {
				return math.Log10(v), true
			} else {
				return e, false
			}
		},
		val: ValidateOneNumberArg,
	}
	builtins["mod"] = builtinFunc{
		fType: FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := toF64(args[0]); e == nil {
				if v1, e1 := toF64(args[1]); e == nil {
					return math.Mod(v, v1), true
				} else {
					return e1, false
				}
			} else {
				return e, false
			}
		},
		val: ValidateTwoNumberArg,
	}
	builtins["power"] = builtinFunc{
		fType: FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v1, e := toF64(args[0]); e == nil {
				if v2, e2 := toF64(args[1]); e2 == nil {
					return math.Pow(v1, v2), true
				} else {
					return e2, false
				}
			} else {
				return e, false
			}
		},
		val: ValidateTwoNumberArg,
	}
	builtins["rand"] = builtinFunc{
		fType: FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			return rand.Float64(), true
		},
		val: ValidateOneArg,
	}
	builtins["round"] = builtinFunc{
		fType: FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := toF64(args[0]); e == nil {
				return math.Round(v), true
			} else {
				return e, false
			}
		},
		val: ValidateOneNumberArg,
	}
	builtins["sign"] = builtinFunc{
		fType: FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := toF64(args[0]); e == nil {
				if v > 0 {
					return 1, true
				} else if v < 0 {
					return -1, true
				} else {
					return 0, true
				}
			} else {
				return e, false
			}
		},
		val: ValidateOneNumberArg,
	}
	builtins["sin"] = builtinFunc{
		fType: FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := toF64(args[0]); e == nil {
				return math.Sin(v), true
			} else {
				return e, false
			}
		},
		val: ValidateOneNumberArg,
	}
	builtins["sinh"] = builtinFunc{
		fType: FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := toF64(args[0]); e == nil {
				return math.Sinh(v), true
			} else {
				return e, false
			}
		},
		val: ValidateOneNumberArg,
	}
	builtins["sqrt"] = builtinFunc{
		fType: FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := toF64(args[0]); e == nil {
				return math.Sqrt(v), true
			} else {
				return e, false
			}
		},
		val: ValidateOneNumberArg,
	}
	builtins["tan"] = builtinFunc{
		fType: FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := toF64(args[0]); e == nil {
				return math.Tan(v), true
			} else {
				return e, false
			}
		},
		val: ValidateOneNumberArg,
	}
	builtins["tanh"] = builtinFunc{
		fType: FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := toF64(args[0]); e == nil {
				return math.Tanh(v), true
			} else {
				return e, false
			}
		},
		val: ValidateOneNumberArg,
	}
}

func toF64(arg interface{}) (float64, error) {
	if v, ok := arg.(float64); ok {
		return v, nil
	} else if v, ok := arg.(int); ok {
		return float64(v), nil
	}
	return 0, fmt.Errorf("only float64 & int type are supported")
}
