// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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
	"errors"
	"fmt"
	"math"
	"math/big"
	"math/cmplx"
	"math/rand"
	"strconv"
	"strings"
	"unicode"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
)

const (
	RadToDeg = 180 / math.Pi
	DegToRad = math.Pi / 180
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
				r := math.Log(v)
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
			v, e := cast.ToFloat64(args[0], cast.CONVERT_SAMEKIND)
			if e != nil {
				return e, false
			}

			var r float64
			if len(args) == 1 {
				r = math.Log10(v)
			} else {
				x, e := cast.ToFloat64(args[1], cast.CONVERT_SAMEKIND)
				if e != nil {
					return e, false
				}
				r = math.Log(x) / math.Log(v)
			}

			if !math.IsNaN(r) {
				return r, true
			}
			return nil, true
		},
		val: func(_ api.FunctionContext, args []ast.Expr) error {
			if len(args) != 1 && len(args) != 2 {
				return errors.New("Expect 1 or 2 arguments only")
			}
			if ast.IsStringArg(args[0]) || ast.IsTimeArg(args[0]) || ast.IsBooleanArg(args[0]) {
				return ProduceErrInfo(0, "number - float or int")
			}
			return nil
		},
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
		exec: func(ctx api.FunctionContext, args []any) (any, bool) {
			var e error
			precision := 0
			if len(args) > 1 {
				precision, e = cast.ToInt(args[1], cast.CONVERT_SAMEKIND)
				if e != nil {
					return fmt.Errorf("The second argument must be an integer: %v", e), false
				}
			}
			v, e := cast.ToFloat64(args[0], cast.CONVERT_SAMEKIND)
			if e != nil {
				return e, false
			}
			factor := math.Pow(10, float64(precision))
			scaled := v * factor
			if math.IsInf(scaled, 0) || math.IsInf(factor, 0) {
				// Overflow detected - fall back to big.Float
				return roundWithBigFloat(v, precision), true
			}
			return math.Round(scaled) / factor, true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			if len(args) != 1 && len(args) != 2 {
				return errors.New("Expect 1 or 2 arguments only")
			}
			if ast.IsStringArg(args[0]) || ast.IsTimeArg(args[0]) || ast.IsBooleanArg(args[0]) {
				return ProduceErrInfo(0, "number - float or int")
			}
			if len(args) == 2 {
				if ast.IsStringArg(args[1]) || ast.IsTimeArg(args[1]) || ast.IsBooleanArg(args[1]) {
					return ProduceErrInfo(1, "number - float or int")
				}
			}
			return nil
		},
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
	builtins["cot"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := cast.ToFloat64(args[0], cast.CONVERT_SAMEKIND); e == nil {
				r := real(cmplx.Cot(complex(v, 0)))
				if math.IsNaN(r) {
					return nil, true
				} else if math.IsInf(r, 0) {
					return errors.New("out-of-range error"), false
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
	builtins["radians"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := cast.ToFloat64(args[0], cast.CONVERT_SAMEKIND); e == nil {
				r := radians(v)
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
	builtins["degrees"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			if v, e := cast.ToFloat64(args[0], cast.CONVERT_SAMEKIND); e == nil {
				r := degrees(v)
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
	builtins["conv"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0 := cast.ToStringAlways(args[0])
			arg1, _ := cast.ToInt64(args[1], cast.CONVERT_SAMEKIND)
			arg2, _ := cast.ToInt64(args[2], cast.CONVERT_SAMEKIND)

			res, isNull, err := conv(arg0, arg1, arg2)
			if err != nil {
				return err, false
			}
			if isNull {
				return nil, true
			}
			return res, true
		},

		val: func(_ api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(3, len(args)); err != nil {
				return err
			}
			if !ast.IsIntegerArg(args[1]) {
				return ProduceErrInfo(1, "integer")
			}
			if !ast.IsIntegerArg(args[2]) {
				return ProduceErrInfo(2, "integer")
			}
			return nil
		},
		check: returnNilIfHasAnyNil,
	}
}

func radians(degrees float64) float64 {
	return degrees * (DegToRad)
}

func degrees(radians float64) float64 {
	return radians * (RadToDeg)
}

func conv(str string, fromBase, toBase int64) (res string, isNull bool, err error) {
	var (
		signed     bool
		negative   bool
		ignoreSign bool
	)
	if fromBase < 0 {
		fromBase = -fromBase
		signed = true
	}

	if toBase < 0 {
		toBase = -toBase
		ignoreSign = true
	}

	if fromBase > 36 || fromBase < 2 || toBase > 36 || toBase < 2 {
		return res, true, nil
	}

	str = getValidPrefix(strings.TrimSpace(str), fromBase)
	if len(str) == 0 {
		return "0", false, nil
	}

	if str[0] == '-' {
		negative = true
		str = str[1:]
	}

	val, err := strconv.ParseUint(str, int(fromBase), 64)
	if err != nil {
		return res, false, err
	}
	if signed {
		if negative && val > -math.MinInt64 {
			val = -math.MinInt64
		}
		if !negative && val > math.MaxInt64 {
			val = math.MaxInt64
		}
	}
	if negative {
		val = -val
	}

	if val > math.MaxInt64 {
		conf.Log.Warnf("value %d is out of int64 range", val)
	}
	if int64(val) < 0 {
		negative = true
	} else {
		negative = false
	}
	if ignoreSign && negative {
		val = 0 - val
	}

	s := strconv.FormatUint(val, int(toBase))
	if negative && ignoreSign {
		s = "-" + s
	}
	res = strings.ToUpper(s)
	return res, false, nil
}

// getValidPrefix gets a prefix of string which can parsed to a number with base. the minimum base is 2 and the maximum is 36.
func getValidPrefix(s string, base int64) string {
	var (
		validLen int
		upper    rune
	)
	switch {
	case base >= 2 && base <= 9:
		upper = rune('0' + base)
	case base <= 36:
		upper = rune('A' + base - 10)
	default:
		return ""
	}
Loop:
	for i := 0; i < len(s); i++ {
		c := rune(s[i])
		switch {
		case unicode.IsDigit(c) || unicode.IsLower(c) || unicode.IsUpper(c):
			c = unicode.ToUpper(c)
			if c < upper {
				validLen = i + 1
			} else {
				break Loop
			}
		case c == '+' || c == '-':
			if i != 0 {
				break Loop
			}
		default:
			break Loop
		}
	}
	if validLen > 1 && s[0] == '+' {
		return s[1:validLen]
	}
	return s[:validLen]
}

// roundWithBigFloat handles rounding using arbitrary precision arithmetic
func roundWithBigFloat(v float64, precision int) float64 {
	const bigFloatPrec = 256 // Precision in bits for big.Float

	// Convert to big.Float with high precision
	bf := big.NewFloat(v).SetPrec(bigFloatPrec)

	// Create multiplier as 10^precision
	multiplier := new(big.Float).SetPrec(bigFloatPrec)
	ten := big.NewFloat(10).SetPrec(bigFloatPrec)

	if precision == 0 {
		multiplier.SetInt64(1)
	} else if precision > 0 {
		// Positive precision: 10^precision
		multiplier.Copy(ten)
		for i := 1; i < precision; i++ {
			multiplier.Mul(multiplier, ten)
		}
	} else {
		// Negative precision: 10^precision = 1 / 10^abs(precision)
		multiplier.SetInt64(1)
		for i := 0; i < -precision; i++ {
			multiplier.Quo(multiplier, ten)
		}
	}

	// Multiply value by multiplier
	scaled := new(big.Float).SetPrec(bigFloatPrec)
	scaled.Mul(bf, multiplier)

	// Round to nearest integer (away from zero for .5)
	intPart := new(big.Int)
	scaled.Int(intPart)

	// Get fractional part
	fracPart := new(big.Float).SetPrec(bigFloatPrec)
	fracPart.Sub(scaled, new(big.Float).SetInt(intPart))

	// Check if we need to round up or down
	half := big.NewFloat(0.5).SetPrec(bigFloatPrec)
	negHalf := big.NewFloat(-0.5).SetPrec(bigFloatPrec)

	if fracPart.Cmp(half) >= 0 {
		// Round up for positive
		intPart.Add(intPart, big.NewInt(1))
	} else if fracPart.Cmp(negHalf) <= 0 {
		// Round down for negative (away from zero)
		intPart.Sub(intPart, big.NewInt(1))
	}

	// Convert back and divide by multiplier
	result := new(big.Float).SetPrec(bigFloatPrec)
	result.SetInt(intPart)
	result.Quo(result, multiplier)

	// Convert back to float64
	f64, _ := result.Float64()
	return f64
}
