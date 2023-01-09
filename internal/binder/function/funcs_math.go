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
	"math"
	"math/rand"
)

func registerMathFunc() {
	builtins["abs"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			switch v := args[0].(type) {
			case int:
				return int(math.Abs(float64(v))), true
			case int64:
				return int64(math.Abs(float64(v))), true
			case float64:
				return math.Abs(v), true
			default:
				if vi, err := cast.ToInt(v, cast.STRICT); err == nil {
					return int(math.Abs(float64(vi))), true
				}
				if vf, err := cast.ToFloat64(v, cast.STRICT); err == nil {
					return math.Abs(vf), true
				}
				return fmt.Errorf("only float64 & int type are supported"), false
			}
		},
		val: ValidateOneNumberArg,
	}
	builtins["acos"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := cast.ToFloat64(args[0], cast.CONVERT_SAMEKIND); e == nil {
				return math.Acos(v), true
			} else {
				return e, false
			}
		},
		val: ValidateOneNumberArg,
	}
	builtins["asin"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := cast.ToFloat64(args[0], cast.CONVERT_SAMEKIND); e == nil {
				return math.Asin(v), true
			} else {
				return e, false
			}
		},
		val: ValidateOneNumberArg,
	}
	builtins["atan"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := cast.ToFloat64(args[0], cast.CONVERT_SAMEKIND); e == nil {
				return math.Atan(v), true
			} else {
				return e, false
			}
		},
		val: ValidateOneNumberArg,
	}
	builtins["atan2"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v1, e := cast.ToFloat64(args[0], cast.CONVERT_SAMEKIND); e == nil {
				if v2, e1 := cast.ToFloat64(args[1], cast.CONVERT_SAMEKIND); e1 == nil {
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
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			v1, err := cast.ToInt(args[0], cast.STRICT)
			if err != nil {
				return fmt.Errorf("Expect int type for the first operand but got %v", args[0]), false
			}
			v2, err := cast.ToInt(args[0], cast.STRICT)
			if err != nil {
				return fmt.Errorf("Expect int type for the second operand but got %v", args[1]), false
			}
			return v1 & v2, true
		},
		val: ValidateTwoIntArg,
	}
	builtins["bitor"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			v1, err := cast.ToInt(args[0], cast.STRICT)
			if err != nil {
				return fmt.Errorf("Expect int type for the first operand but got %v", args[0]), false
			}
			v2, err := cast.ToInt(args[0], cast.STRICT)
			if err != nil {
				return fmt.Errorf("Expect int type for the second operand but got %v", args[1]), false
			}
			return v1 | v2, true
		},
		val: ValidateTwoIntArg,
	}
	builtins["bitxor"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			v1, err := cast.ToInt(args[0], cast.STRICT)
			if err != nil {
				return fmt.Errorf("Expect int type for the first operand but got %v", args[0]), false
			}
			v2, err := cast.ToInt(args[0], cast.STRICT)
			if err != nil {
				return fmt.Errorf("Expect int type for the second operand but got %v", args[1]), false
			}
			return v1 ^ v2, true
		},
		val: ValidateTwoIntArg,
	}
	builtins["bitnot"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			v1, err := cast.ToInt(args[0], cast.STRICT)
			if err != nil {
				return fmt.Errorf("Expect int type for operand but got %v", args[0]), false
			}
			return ^v1, true
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
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := cast.ToFloat64(args[0], cast.CONVERT_SAMEKIND); e == nil {
				return math.Ceil(v), true
			} else {
				return e, false
			}
		},
		val: ValidateOneNumberArg,
	}
	builtins["cos"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := cast.ToFloat64(args[0], cast.CONVERT_SAMEKIND); e == nil {
				return math.Cos(v), true
			} else {
				return e, false
			}
		},
		val: ValidateOneNumberArg,
	}
	builtins["cosh"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := cast.ToFloat64(args[0], cast.CONVERT_SAMEKIND); e == nil {
				return math.Cosh(v), true
			} else {
				return e, false
			}
		},
		val: ValidateOneNumberArg,
	}
	builtins["exp"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := cast.ToFloat64(args[0], cast.CONVERT_SAMEKIND); e == nil {
				return math.Exp(v), true
			} else {
				return e, false
			}
		},
		val: ValidateOneNumberArg,
	}
	builtins["ln"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := cast.ToFloat64(args[0], cast.CONVERT_SAMEKIND); e == nil {
				return math.Log2(v), true
			} else {
				return e, false
			}
		},
		val: ValidateOneNumberArg,
	}
	builtins["log"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := cast.ToFloat64(args[0], cast.CONVERT_SAMEKIND); e == nil {
				return math.Log10(v), true
			} else {
				return e, false
			}
		},
		val: ValidateOneNumberArg,
	}
	builtins["mod"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := cast.ToFloat64(args[0], cast.CONVERT_SAMEKIND); e == nil {
				if v1, e1 := cast.ToFloat64(args[1], cast.CONVERT_SAMEKIND); e == nil {
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
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v1, e := cast.ToFloat64(args[0], cast.CONVERT_SAMEKIND); e == nil {
				if v2, e2 := cast.ToFloat64(args[1], cast.CONVERT_SAMEKIND); e2 == nil {
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
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			return rand.Float64(), true
		},
		val: ValidateOneArg,
	}
	builtins["round"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := cast.ToFloat64(args[0], cast.CONVERT_SAMEKIND); e == nil {
				return math.Round(v), true
			} else {
				return e, false
			}
		},
		val: ValidateOneNumberArg,
	}
	builtins["sign"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := cast.ToFloat64(args[0], cast.CONVERT_SAMEKIND); e == nil {
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
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := cast.ToFloat64(args[0], cast.CONVERT_SAMEKIND); e == nil {
				return math.Sin(v), true
			} else {
				return e, false
			}
		},
		val: ValidateOneNumberArg,
	}
	builtins["sinh"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := cast.ToFloat64(args[0], cast.CONVERT_SAMEKIND); e == nil {
				return math.Sinh(v), true
			} else {
				return e, false
			}
		},
		val: ValidateOneNumberArg,
	}
	builtins["sqrt"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := cast.ToFloat64(args[0], cast.CONVERT_SAMEKIND); e == nil {
				return math.Sqrt(v), true
			} else {
				return e, false
			}
		},
		val: ValidateOneNumberArg,
	}
	builtins["tan"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := cast.ToFloat64(args[0], cast.CONVERT_SAMEKIND); e == nil {
				return math.Tan(v), true
			} else {
				return e, false
			}
		},
		val: ValidateOneNumberArg,
	}
	builtins["tanh"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := cast.ToFloat64(args[0], cast.CONVERT_SAMEKIND); e == nil {
				return math.Tanh(v), true
			} else {
				return e, false
			}
		},
		val: ValidateOneNumberArg,
	}
}
