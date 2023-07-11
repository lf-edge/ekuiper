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
	"math/rand"

	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/cast"
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
		val:   ValidateOneNumberArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["acos"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := cast.ToFloat64(args[0], cast.CONVERT_SAMEKIND); e == nil {
				r := math.Acos(v)
				if math.IsNaN(r) {
					return nil, true
				} else {
					return r, true
				}
			} else {
				return e, false
			}
		},
		val:   ValidateOneNumberArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["asin"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := cast.ToFloat64(args[0], cast.CONVERT_SAMEKIND); e == nil {
				r := math.Asin(v)
				if math.IsNaN(r) {
					return nil, true
				} else {
					return r, true
				}
			} else {
				return e, false
			}
		},
		val:   ValidateOneNumberArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["atan"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := cast.ToFloat64(args[0], cast.CONVERT_SAMEKIND); e == nil {
				r := math.Atan(v)
				if math.IsNaN(r) {
					return nil, true
				} else {
					return r, true
				}
			} else {
				return e, false
			}
		},
		val:   ValidateOneNumberArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["atan2"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v1, e := cast.ToFloat64(args[0], cast.CONVERT_SAMEKIND); e == nil {
				if v2, e1 := cast.ToFloat64(args[1], cast.CONVERT_SAMEKIND); e1 == nil {
					r := math.Atan2(v1, v2)
					if math.IsNaN(r) {
						return nil, true
					} else {
						return r, true
					}
				} else {
					return e1, false
				}
			} else {
				return e, false
			}
		},
		val:   ValidateTwoNumberArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["bitand"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			v1, err := cast.ToInt(args[0], cast.STRICT)
			if err != nil {
				return fmt.Errorf("Expect int type for the first operand but got %v", args[0]), false
			}
			v2, err := cast.ToInt(args[1], cast.STRICT)
			if err != nil {
				return fmt.Errorf("Expect int type for the second operand but got %v", args[1]), false
			}
			return v1 & v2, true
		},
		val:   ValidateTwoIntArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["bitor"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			v1, err := cast.ToInt(args[0], cast.STRICT)
			if err != nil {
				return fmt.Errorf("Expect int type for the first operand but got %v", args[0]), false
			}
			v2, err := cast.ToInt(args[1], cast.STRICT)
			if err != nil {
				return fmt.Errorf("Expect int type for the second operand but got %v", args[1]), false
			}
			return v1 | v2, true
		},
		val:   ValidateTwoIntArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["bitxor"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			v1, err := cast.ToInt(args[0], cast.STRICT)
			if err != nil {
				return fmt.Errorf("Expect int type for the first operand but got %v", args[0]), false
			}
			v2, err := cast.ToInt(args[1], cast.STRICT)
			if err != nil {
				return fmt.Errorf("Expect int type for the second operand but got %v", args[1]), false
			}
			return v1 ^ v2, true
		},
		val:   ValidateTwoIntArg,
		check: returnNilIfHasAnyNil,
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
		check: returnNilIfHasAnyNil,
	}
	builtins["ceiling"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := cast.ToFloat64(args[0], cast.CONVERT_SAMEKIND); e == nil {
				return math.Ceil(v), true
			} else {
				return e, false
			}
		},
		val:   ValidateOneNumberArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["ceil"] = builtins["ceiling"] // Synonym for CEILING.
	builtins["cos"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := cast.ToFloat64(args[0], cast.CONVERT_SAMEKIND); e == nil {
				r := math.Cos(v)
				if math.IsNaN(r) {
					return nil, true
				} else {
					return r, true
				}
			} else {
				return e, false
			}
		},
		val:   ValidateOneNumberArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["cosh"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := cast.ToFloat64(args[0], cast.CONVERT_SAMEKIND); e == nil {
				r := math.Cosh(v)
				if math.IsNaN(r) {
					return nil, true
				} else {
					return r, true
				}
			} else {
				return e, false
			}
		},
		val:   ValidateOneNumberArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["exp"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := cast.ToFloat64(args[0], cast.CONVERT_SAMEKIND); e == nil {
				r := math.Exp(v)
				if math.IsNaN(r) {
					return nil, true
				} else {
					return r, true
				}
			} else {
				return e, false
			}
		},
		val:   ValidateOneNumberArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["floor"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := cast.ToFloat64(args[0], cast.CONVERT_SAMEKIND); e == nil {
				return math.Floor(v), true
			} else {
				return e, false
			}
		},
		val:   ValidateOneNumberArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["ln"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := cast.ToFloat64(args[0], cast.CONVERT_SAMEKIND); e == nil {
				r := math.Log2(v)
				if math.IsNaN(r) {
					return nil, true
				} else {
					return r, true
				}
			} else {
				return e, false
			}
		},
		val:   ValidateOneNumberArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["log"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := cast.ToFloat64(args[0], cast.CONVERT_SAMEKIND); e == nil {
				r := math.Log10(v)
				if math.IsNaN(r) {
					return nil, true
				} else {
					return r, true
				}
			} else {
				return e, false
			}
		},
		val:   ValidateOneNumberArg,
		check: returnNilIfHasAnyNil,
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
		val:   ValidateTwoNumberArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["pi"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(_ api.FunctionContext, _ []interface{}) (interface{}, bool) {
			return math.Pi, true
		},
		val:   ValidateNoArg,
		check: returnNilIfHasAnyNil,
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
		val:   ValidateTwoNumberArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["pow"] = builtins["power"] // Synonym for POWER.
	builtins["rand"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			return rand.Float64(), true
		},
		val: ValidateNoArg,
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
		val:   ValidateOneNumberArg,
		check: returnNilIfHasAnyNil,
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
		val:   ValidateOneNumberArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["sin"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := cast.ToFloat64(args[0], cast.CONVERT_SAMEKIND); e == nil {
				r := math.Sin(v)
				if math.IsNaN(r) {
					return nil, true
				} else {
					return r, true
				}
			} else {
				return e, false
			}
		},
		val:   ValidateOneNumberArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["sinh"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := cast.ToFloat64(args[0], cast.CONVERT_SAMEKIND); e == nil {
				r := math.Sinh(v)
				if math.IsNaN(r) {
					return nil, true
				} else {
					return r, true
				}
			} else {
				return e, false
			}
		},
		val:   ValidateOneNumberArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["sqrt"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := cast.ToFloat64(args[0], cast.CONVERT_SAMEKIND); e == nil {
				r := math.Sqrt(v)
				if math.IsNaN(r) {
					return nil, true
				} else {
					return r, true
				}
			} else {
				return e, false
			}
		},
		val:   ValidateOneNumberArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["tan"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := cast.ToFloat64(args[0], cast.CONVERT_SAMEKIND); e == nil {
				r := math.Tan(v)
				if math.IsNaN(r) {
					return nil, true
				} else {
					return r, true
				}
			} else {
				return e, false
			}
		},
		val:   ValidateOneNumberArg,
		check: returnNilIfHasAnyNil,
	}
	builtins["tanh"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := cast.ToFloat64(args[0], cast.CONVERT_SAMEKIND); e == nil {
				r := math.Tanh(v)
				if math.IsNaN(r) {
					return nil, true
				} else {
					return r, true
				}
			} else {
				return e, false
			}
		},
		val:   ValidateOneNumberArg,
		check: returnNilIfHasAnyNil,
	}
}
